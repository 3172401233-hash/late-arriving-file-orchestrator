"""Batch file orchestrator.

Consumes file-arrival events against an expected manifest and only produces a
committable batch once every required file is complete and digest-consistent.

Design goals:
- Consumers never see partial or mixed-version data: a batch is published
  atomically and the consumer-visible snapshot only changes after a commit
  fully succeeds.
- Duplicate deliveries are idempotent no-ops.
- Stale shards (wrong sequence) and unknown files are quarantined, never
  silently mixed into a batch.
- Half-written files are tracked as incomplete; a later full overwrite
  replaces them.
- An interrupted commit can be rolled back via ``recover()`` and retried.
- After ``cleanup()``, replaying the same arrival events is a no-op.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Tuple


class IngestStatus(Enum):
    ACCEPTED = "accepted"                    # file is now complete
    PARTIAL = "partial"                      # incomplete (half file), awaiting overwrite
    DUPLICATE = "duplicate"                  # identical re-delivery, no-op
    IGNORED_COMMITTED = "ignored_committed"  # replay after commit/cleanup, no-op
    QUARANTINED_UNKNOWN = "quarantined_unknown"    # not in the manifest
    QUARANTINED_STALE = "quarantined_stale"        # sequence older than expected
    QUARANTINED_FUTURE = "quarantined_future"      # sequence newer than expected
    QUARANTINED_CORRUPT = "quarantined_corrupt"    # digest mismatch
    QUARANTINED_CONFLICT = "quarantined_conflict"  # conflicts with completed file


class SlotState(Enum):
    PENDING = "pending"
    PARTIAL = "partial"
    COMPLETE = "complete"


class CommitState(Enum):
    IDLE = "idle"              # no commit in flight
    COMMITTING = "committing"  # interrupted mid-commit, recover() required
    COMMITTED = "committed"


class CommitInterrupted(RuntimeError):
    """Raised when a commit is interrupted before the publish pointer flips."""


class BatchNotReady(RuntimeError):
    """Raised when building a batch before all required files are complete."""


@dataclass(frozen=True)
class FileSpec:
    """One expected file in the manifest."""

    name: str
    size: int
    sha256: str
    sequence: int
    required: bool = True

    @classmethod
    def from_content(
        cls, name: str, content: bytes, sequence: int, required: bool = True
    ) -> "FileSpec":
        return cls(
            name=name,
            size=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
            sequence=sequence,
            required=required,
        )


@dataclass(frozen=True)
class Arrival:
    """A file-arrival event observed by the orchestrator."""

    name: str
    sequence: int
    content: bytes

    @property
    def size(self) -> int:
        return len(self.content)

    @property
    def sha256(self) -> str:
        return hashlib.sha256(self.content).hexdigest()


@dataclass(frozen=True)
class QuarantineRecord:
    name: str
    sequence: int
    size: int
    sha256: str
    reason: str


@dataclass
class _Slot:
    spec: FileSpec
    state: SlotState = SlotState.PENDING
    observed_size: int = 0
    observed_sha256: Optional[str] = None

    @property
    def complete(self) -> bool:
        return self.state is SlotState.COMPLETE


@dataclass(frozen=True)
class BatchFile:
    name: str
    size: int
    sha256: str
    sequence: int
    required: bool
    content: bytes


@dataclass(frozen=True)
class Batch:
    """An immutable, committable snapshot of a complete manifest."""

    batch_id: str
    sequence: int
    files: Tuple[BatchFile, ...]

    @property
    def digest(self) -> str:
        h = hashlib.sha256()
        for f in self.files:
            h.update(f.name.encode())
            h.update(f.sha256.encode())
        return h.hexdigest()

    def file_names(self) -> Tuple[str, ...]:
        return tuple(f.name for f in self.files)


class BatchOrchestrator:
    """Tracks arrivals for one manifest and publishes atomic batches."""

    def __init__(self, batch_id: str, manifest: List[FileSpec]):
        if not manifest:
            raise ValueError("manifest must not be empty")
        names = [s.name for s in manifest]
        if len(names) != len(set(names)):
            raise ValueError("manifest contains duplicate file names")
        self.batch_id = batch_id
        self.sequence = max(s.sequence for s in manifest)
        self._slots: Dict[str, _Slot] = {s.name: _Slot(spec=s) for s in manifest}
        self._contents: Dict[str, bytes] = {}
        self._quarantine: List[QuarantineRecord] = []
        self._batch: Optional[Batch] = None
        self._commit_state = CommitState.IDLE
        self._staged: Dict[str, BatchFile] = {}
        self._journal: List[str] = []
        self._published: Optional[Batch] = None
        self._committed_digests: Dict[str, str] = {}

    # ------------------------------------------------------------------ ingest

    def ingest(self, arrival: Arrival) -> IngestStatus:
        slot = self._slots.get(arrival.name)

        # Replay after a commit/cleanup: identical content is a no-op.
        if self._commit_state is CommitState.COMMITTED:
            committed = self._committed_digests.get(arrival.name)
            if committed is not None and committed == arrival.sha256:
                return IngestStatus.IGNORED_COMMITTED

        if slot is None:
            return self._quarantine_arrival(
                arrival, IngestStatus.QUARANTINED_UNKNOWN,
                "file not present in manifest")

        spec = slot.spec
        if arrival.sequence < spec.sequence:
            return self._quarantine_arrival(
                arrival, IngestStatus.QUARANTINED_STALE,
                f"sequence {arrival.sequence} < expected {spec.sequence}")
        if arrival.sequence > spec.sequence:
            return self._quarantine_arrival(
                arrival, IngestStatus.QUARANTINED_FUTURE,
                f"sequence {arrival.sequence} > expected {spec.sequence}")

        if slot.complete:
            if slot.observed_sha256 == arrival.sha256:
                return IngestStatus.DUPLICATE
            return self._quarantine_arrival(
                arrival, IngestStatus.QUARANTINED_CONFLICT,
                "conflicts with already-completed file")

        if arrival.size != spec.size:
            # Half-written file: remember it, wait for a full overwrite.
            slot.state = SlotState.PARTIAL
            slot.observed_size = arrival.size
            slot.observed_sha256 = arrival.sha256
            self._contents.pop(arrival.name, None)
            return IngestStatus.PARTIAL

        if arrival.sha256 != spec.sha256:
            slot.state = SlotState.PARTIAL
            slot.observed_size = arrival.size
            slot.observed_sha256 = arrival.sha256
            self._contents.pop(arrival.name, None)
            return self._quarantine_arrival(
                arrival, IngestStatus.QUARANTINED_CORRUPT,
                "digest mismatch with manifest")

        slot.state = SlotState.COMPLETE
        slot.observed_size = arrival.size
        slot.observed_sha256 = arrival.sha256
        self._contents[arrival.name] = arrival.content
        return IngestStatus.ACCEPTED

    def _quarantine_arrival(
        self, arrival: Arrival, status: IngestStatus, reason: str
    ) -> IngestStatus:
        self._quarantine.append(QuarantineRecord(
            name=arrival.name,
            sequence=arrival.sequence,
            size=arrival.size,
            sha256=arrival.sha256,
            reason=reason,
        ))
        return status

    # ------------------------------------------------------------------ status

    def is_ready(self) -> bool:
        """True when every required file is complete and digest-consistent."""
        return all(
            slot.complete
            for slot in self._slots.values()
            if slot.spec.required
        )

    def missing_required(self) -> List[str]:
        return sorted(
            name for name, slot in self._slots.items()
            if slot.spec.required and not slot.complete
        )

    @property
    def quarantine(self) -> Tuple[QuarantineRecord, ...]:
        return tuple(self._quarantine)

    @property
    def commit_state(self) -> CommitState:
        return self._commit_state

    @property
    def journal(self) -> Tuple[str, ...]:
        return tuple(self._journal)

    # ------------------------------------------------------------------- batch

    def build_batch(self) -> Batch:
        """Freeze the current complete files into an immutable batch.

        Idempotent: repeated calls while nothing changed return the same batch.
        """
        if not self.is_ready():
            raise BatchNotReady(
                f"missing required files: {', '.join(self.missing_required())}"
            )
        if self._batch is not None:
            return self._batch
        files = tuple(
            BatchFile(
                name=name,
                size=slot.spec.size,
                sha256=slot.spec.sha256,
                sequence=slot.spec.sequence,
                required=slot.spec.required,
                content=self._contents[name],
            )
            for name, slot in sorted(self._slots.items())
            if slot.complete
        )
        self._batch = Batch(batch_id=self.batch_id, sequence=self.sequence, files=files)
        return self._batch

    def revoke_batch(self) -> None:
        """Discard a built-but-uncommitted batch (e.g. a file turned out bad).

        Only valid before commit starts; a committed batch cannot be revoked
        through this API.
        """
        if self._commit_state is CommitState.COMMITTED:
            raise RuntimeError("cannot revoke an already-committed batch")
        if self._commit_state is CommitState.COMMITTING:
            raise CommitInterrupted("commit in flight; call recover() first")
        self._batch = None

    # ------------------------------------------------------------------ commit

    def commit(self, batch: Optional[Batch] = None, fail_after: Optional[int] = None) -> Batch:
        """Atomically publish a batch.

        Files are staged one by one under a journal; the consumer-visible
        pointer flips only after every file is staged. ``fail_after`` injects
        an interruption after N staged files (testing/simulation). On
        interruption the state becomes COMMITTING and ``recover()`` must be
        called before retrying.
        """
        if self._commit_state is CommitState.COMMITTING:
            raise CommitInterrupted("previous commit interrupted; call recover() first")
        if self._commit_state is CommitState.COMMITTED:
            raise RuntimeError("batch already committed")
        batch = batch or self.build_batch()
        if batch is not self._batch and batch != self._batch:
            raise ValueError("batch does not match the orchestrator's frozen batch")

        self._commit_state = CommitState.COMMITTING
        self._staged = {}
        for index, batch_file in enumerate(batch.files):
            if fail_after is not None and index >= fail_after:
                raise CommitInterrupted(
                    f"commit interrupted after staging {index} file(s)"
                )
            self._staged[batch_file.name] = batch_file
            self._journal.append(f"staged:{batch_file.name}:{batch_file.sha256}")

        # Atomic publish: the snapshot flips only now, never mid-staging.
        self._published = batch
        self._committed_digests = {f.name: f.sha256 for f in batch.files}
        self._commit_state = CommitState.COMMITTED
        self._journal.append(f"published:{batch.batch_id}:{batch.digest}")
        return batch

    def recover(self) -> None:
        """Roll back an interrupted commit.

        Staged-but-unpublished files are discarded; the consumer snapshot is
        untouched (it never changed). The orchestrator returns to IDLE and the
        commit may be retried.
        """
        if self._commit_state is not CommitState.COMMITTING:
            return
        self._staged = {}
        self._journal.append("rollback:interrupted-commit-discarded")
        self._commit_state = CommitState.IDLE

    # ----------------------------------------------------------------- consume

    def snapshot(self) -> Optional[Batch]:
        """Consumer view: the last fully committed batch, or None.

        Never returns partial or in-flight data.
        """
        return self._published

    # ----------------------------------------------------------------- cleanup

    def cleanup(self) -> None:
        """Release staging state after a successful commit.

        Committed digests are retained so that replaying old arrival events
        stays idempotent (they are ignored, not re-ingested).
        """
        if self._commit_state is not CommitState.COMMITTED:
            raise RuntimeError("cleanup is only allowed after a successful commit")
        self._slots.clear()
        self._contents.clear()
        self._quarantine.clear()
        self._staged = {}
        self._batch = None

    # ------------------------------------------------------------------ replay

    def replay(self, arrivals: List[Arrival]) -> List[IngestStatus]:
        """Re-apply a sequence of arrival events (e.g. after cleanup)."""
        return [self.ingest(a) for a in arrivals]
