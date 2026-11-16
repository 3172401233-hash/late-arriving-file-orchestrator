import unittest

from orchestrator import (
    Arrival,
    BatchNotReady,
    BatchOrchestrator,
    CommitInterrupted,
    CommitState,
    FileSpec,
    IngestStatus,
)

SEQ = 42

SHARD_A = b"shard-A payload: rows 1..100\n"
SHARD_B = b"shard-B payload: rows 101..200\n"
SHARD_C = b"shard-C payload: rows 201..300\n"
OPTIONAL_META = b"optional metadata\n"


def make_manifest():
    return [
        FileSpec.from_content("shard-a.dat", SHARD_A, SEQ),
        FileSpec.from_content("shard-b.dat", SHARD_B, SEQ),
        FileSpec.from_content("shard-c.dat", SHARD_C, SEQ),
        FileSpec.from_content("meta.json", OPTIONAL_META, SEQ, required=False),
    ]


def make_orchestrator():
    return BatchOrchestrator("batch-2026-11-16", make_manifest())


def arrive(name, content, sequence=SEQ):
    return Arrival(name=name, sequence=sequence, content=content)


class TestCompleteness(unittest.TestCase):
    def test_batch_not_ready_until_all_required_complete(self):
        orch = make_orchestrator()
        self.assertFalse(orch.is_ready())
        self.assertEqual(
            orch.missing_required(), ["shard-a.dat", "shard-b.dat", "shard-c.dat"]
        )
        with self.assertRaises(BatchNotReady):
            orch.build_batch()

        orch.ingest(arrive("shard-a.dat", SHARD_A))
        orch.ingest(arrive("shard-b.dat", SHARD_B))
        self.assertFalse(orch.is_ready())
        self.assertEqual(orch.missing_required(), ["shard-c.dat"])

        orch.ingest(arrive("shard-c.dat", SHARD_C))
        self.assertTrue(orch.is_ready())

    def test_happy_path_commit_and_snapshot(self):
        orch = make_orchestrator()
        for name, content in [("shard-a.dat", SHARD_A), ("shard-b.dat", SHARD_B),
                              ("shard-c.dat", SHARD_C)]:
            self.assertEqual(orch.ingest(arrive(name, content)), IngestStatus.ACCEPTED)
        self.assertIsNone(orch.snapshot())  # nothing visible before commit

        batch = orch.build_batch()
        orch.commit(batch)
        snap = orch.snapshot()
        self.assertIsNotNone(snap)
        self.assertEqual(snap.file_names(), ("shard-a.dat", "shard-b.dat", "shard-c.dat"))
        self.assertEqual(orch.commit_state, CommitState.COMMITTED)


class TestHalfFileOverwrite(unittest.TestCase):
    def test_partial_file_does_not_complete_slot(self):
        orch = make_orchestrator()
        half = SHARD_A[: len(SHARD_A) // 2]
        status = orch.ingest(arrive("shard-a.dat", half))
        self.assertEqual(status, IngestStatus.PARTIAL)
        self.assertFalse(orch.is_ready())
        self.assertEqual(orch.missing_required(),
                         ["shard-a.dat", "shard-b.dat", "shard-c.dat"])

    def test_full_overwrite_replaces_half_file(self):
        orch = make_orchestrator()
        orch.ingest(arrive("shard-a.dat", SHARD_A[:5]))
        status = orch.ingest(arrive("shard-a.dat", SHARD_A))
        self.assertEqual(status, IngestStatus.ACCEPTED)

        orch.ingest(arrive("shard-b.dat", SHARD_B))
        orch.ingest(arrive("shard-c.dat", SHARD_C))
        batch = orch.build_batch()
        committed = {f.name: f.content for f in batch.files}
        # Consumer sees the full content, never the half file.
        self.assertEqual(committed["shard-a.dat"], SHARD_A)

    def test_corrupt_digest_goes_to_quarantine_and_can_be_overwritten(self):
        orch = make_orchestrator()
        corrupt = b"x" * len(SHARD_A)  # right size, wrong digest
        status = orch.ingest(arrive("shard-a.dat", corrupt))
        self.assertEqual(status, IngestStatus.QUARANTINED_CORRUPT)
        self.assertFalse(orch.is_ready())
        self.assertEqual(len(orch.quarantine), 1)
        self.assertIn("digest mismatch", orch.quarantine[0].reason)

        status = orch.ingest(arrive("shard-a.dat", SHARD_A))
        self.assertEqual(status, IngestStatus.ACCEPTED)


class TestLateShard(unittest.TestCase):
    def test_late_shard_completes_pending_batch(self):
        orch = make_orchestrator()
        orch.ingest(arrive("shard-a.dat", SHARD_A))
        orch.ingest(arrive("shard-b.dat", SHARD_B))
        # shard-c missing: no batch can be built, consumer sees nothing.
        self.assertFalse(orch.is_ready())
        with self.assertRaises(BatchNotReady):
            orch.build_batch()
        self.assertIsNone(orch.snapshot())

        # Late shard arrives later; batch becomes committable.
        orch.ingest(arrive("shard-c.dat", SHARD_C))
        self.assertTrue(orch.is_ready())
        orch.commit()
        self.assertEqual(len(orch.snapshot().files), 3)


class TestIdempotency(unittest.TestCase):
    def test_duplicate_upload_is_noop(self):
        orch = make_orchestrator()
        self.assertEqual(orch.ingest(arrive("shard-a.dat", SHARD_A)),
                         IngestStatus.ACCEPTED)
        self.assertEqual(orch.ingest(arrive("shard-a.dat", SHARD_A)),
                         IngestStatus.DUPLICATE)
        self.assertEqual(len(orch.quarantine), 0)

    def test_build_batch_is_idempotent(self):
        orch = make_orchestrator()
        for name, content in [("shard-a.dat", SHARD_A), ("shard-b.dat", SHARD_B),
                              ("shard-c.dat", SHARD_C)]:
            orch.ingest(arrive(name, content))
        first = orch.build_batch()
        second = orch.build_batch()
        self.assertIs(first, second)

    def test_conflicting_redelivery_is_quarantined_not_mixed(self):
        orch = make_orchestrator()
        orch.ingest(arrive("shard-a.dat", SHARD_A))
        tampered = SHARD_A + b"tampered"
        status = orch.ingest(arrive("shard-a.dat", tampered))
        self.assertEqual(status, IngestStatus.QUARANTINED_CONFLICT)

        orch.ingest(arrive("shard-b.dat", SHARD_B))
        orch.ingest(arrive("shard-c.dat", SHARD_C))
        batch = orch.build_batch()
        committed = {f.name: f.content for f in batch.files}
        # Batch still holds the original version; no mixed content.
        self.assertEqual(committed["shard-a.dat"], SHARD_A)


class TestOptionalFiles(unittest.TestCase):
    def test_missing_optional_does_not_block_batch(self):
        orch = make_orchestrator()
        for name, content in [("shard-a.dat", SHARD_A), ("shard-b.dat", SHARD_B),
                              ("shard-c.dat", SHARD_C)]:
            orch.ingest(arrive(name, content))
        self.assertTrue(orch.is_ready())
        batch = orch.build_batch()
        self.assertNotIn("meta.json", batch.file_names())

    def test_present_optional_is_included(self):
        orch = make_orchestrator()
        for name, content in [("shard-a.dat", SHARD_A), ("shard-b.dat", SHARD_B),
                              ("shard-c.dat", SHARD_C), ("meta.json", OPTIONAL_META)]:
            orch.ingest(arrive(name, content))
        batch = orch.build_batch()
        self.assertIn("meta.json", batch.file_names())


class TestQuarantine(unittest.TestCase):
    def test_unknown_file_is_quarantined(self):
        orch = make_orchestrator()
        status = orch.ingest(arrive("surprise.dat", b"???"))
        self.assertEqual(status, IngestStatus.QUARANTINED_UNKNOWN)
        self.assertEqual(orch.quarantine[0].name, "surprise.dat")
        self.assertFalse(orch.is_ready())

    def test_stale_sequence_is_quarantined(self):
        orch = make_orchestrator()
        status = orch.ingest(arrive("shard-a.dat", SHARD_A, sequence=SEQ - 1))
        self.assertEqual(status, IngestStatus.QUARANTINED_STALE)
        self.assertFalse(orch.is_ready())
        # The correct-sequence file is still accepted afterwards.
        self.assertEqual(orch.ingest(arrive("shard-a.dat", SHARD_A)),
                         IngestStatus.ACCEPTED)

    def test_future_sequence_is_quarantined(self):
        orch = make_orchestrator()
        status = orch.ingest(arrive("shard-a.dat", SHARD_A, sequence=SEQ + 1))
        self.assertEqual(status, IngestStatus.QUARANTINED_FUTURE)
        self.assertFalse(orch.is_ready())


class TestCommitInterruption(unittest.TestCase):
    def _ready_orchestrator(self):
        orch = make_orchestrator()
        for name, content in [("shard-a.dat", SHARD_A), ("shard-b.dat", SHARD_B),
                              ("shard-c.dat", SHARD_C)]:
            orch.ingest(arrive(name, content))
        return orch

    def test_interrupted_commit_never_reaches_consumer(self):
        orch = self._ready_orchestrator()
        with self.assertRaises(CommitInterrupted):
            orch.commit(fail_after=1)
        self.assertEqual(orch.commit_state, CommitState.COMMITTING)
        # Consumer snapshot is untouched by the half-staged commit.
        self.assertIsNone(orch.snapshot())

    def test_recover_rolls_back_and_commit_can_be_retried(self):
        orch = self._ready_orchestrator()
        with self.assertRaises(CommitInterrupted):
            orch.commit(fail_after=2)
        orch.recover()
        self.assertEqual(orch.commit_state, CommitState.IDLE)
        self.assertIsNone(orch.snapshot())

        orch.commit()
        self.assertEqual(orch.commit_state, CommitState.COMMITTED)
        self.assertEqual(len(orch.snapshot().files), 3)
        self.assertTrue(any("rollback" in entry for entry in orch.journal))

    def test_commit_while_interrupted_raises(self):
        orch = self._ready_orchestrator()
        with self.assertRaises(CommitInterrupted):
            orch.commit(fail_after=0)
        with self.assertRaises(CommitInterrupted):
            orch.commit()

    def test_revoke_built_batch_before_commit(self):
        orch = self._ready_orchestrator()
        batch = orch.build_batch()
        orch.revoke_batch()
        rebuilt = orch.build_batch()
        self.assertIsNot(batch, rebuilt)
        self.assertEqual(batch.digest, rebuilt.digest)


class TestCleanupAndReplay(unittest.TestCase):
    def test_cleanup_then_replay_is_idempotent(self):
        orch = make_orchestrator()
        events = [
            arrive("shard-a.dat", SHARD_A),
            arrive("shard-b.dat", SHARD_B),
            arrive("shard-c.dat", SHARD_C),
        ]
        for event in events:
            orch.ingest(event)
        committed = orch.commit()
        orch.cleanup()

        statuses = orch.replay(events)
        self.assertEqual(statuses, [IngestStatus.IGNORED_COMMITTED] * 3)
        # Committed snapshot survives cleanup and replay unchanged.
        self.assertIs(orch.snapshot(), committed)
        self.assertEqual(orch.commit_state, CommitState.COMMITTED)

    def test_cleanup_before_commit_is_rejected(self):
        orch = make_orchestrator()
        with self.assertRaises(RuntimeError):
            orch.cleanup()

    def test_consumer_never_sees_mixed_versions(self):
        # Full lifecycle: partials, duplicates, stale and unknown arrivals all
        # occur before commit; the snapshot only ever exposes the final batch.
        orch = make_orchestrator()
        orch.ingest(arrive("shard-a.dat", SHARD_A[:3]))                 # half file
        orch.ingest(arrive("ghost.dat", b"boo"))                        # unknown
        orch.ingest(arrive("shard-b.dat", SHARD_B, sequence=SEQ - 9))   # stale
        orch.ingest(arrive("shard-a.dat", SHARD_A))                     # overwrite
        orch.ingest(arrive("shard-a.dat", SHARD_A))                     # duplicate
        orch.ingest(arrive("shard-b.dat", SHARD_B))
        orch.ingest(arrive("shard-c.dat", SHARD_C))
        self.assertIsNone(orch.snapshot())

        orch.commit()
        snap = orch.snapshot()
        contents = {f.name: f.content for f in snap.files}
        self.assertEqual(contents, {
            "shard-a.dat": SHARD_A,
            "shard-b.dat": SHARD_B,
            "shard-c.dat": SHARD_C,
        })
        self.assertEqual(len(orch.quarantine), 2)  # ghost + stale shard-b


if __name__ == "__main__":
    unittest.main()
