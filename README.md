# late-arriving-file-orchestrator

Python 迟到文件接入编排库，处理预期清单、缺口补齐、重复到达和原子批提交。

供应商每天承诺上传一组分片文件，但偶尔会缺 shard、上传半文件后覆盖、重复推送。
本库确保：只有所有必需文件完整且摘要一致时才生成可提交批次，消费方永远不会
看到半份数据或混合版本。

## 快速开始

```python
from orchestrator import Arrival, BatchOrchestrator, FileSpec

manifest = [
    FileSpec.from_content("shard-a.dat", b"...", sequence=42),
    FileSpec.from_content("shard-b.dat", b"...", sequence=42),
    FileSpec.from_content("meta.json", b"...", sequence=42, required=False),
]
orch = BatchOrchestrator("batch-2026-11-16", manifest)

orch.ingest(Arrival(name="shard-a.dat", sequence=42, content=b"..."))
orch.ingest(Arrival(name="shard-b.dat", sequence=42, content=b"..."))

if orch.is_ready():
    batch = orch.build_batch()   # 冻结不可变批次
    orch.commit(batch)           # 原子发布
    snapshot = orch.snapshot()   # 消费方视图
```

## 到达事件处理规则

| 到达情况 | 结果 |
| --- | --- |
| 完整且摘要一致 | `ACCEPTED`，文件槽位标记为完成 |
| 半文件（大小不符） | `PARTIAL`，等待后续完整覆盖 |
| 大小符合但摘要不符 | `QUARANTINED_CORRUPT`，进入隔离区，可被正确文件覆盖 |
| 与清单序列号不符（过旧/过新） | `QUARANTINED_STALE` / `QUARANTINED_FUTURE`，进入隔离区 |
| 清单之外的未知文件 | `QUARANTINED_UNKNOWN`，进入隔离区 |
| 与已完成文件完全相同的重复上传 | `DUPLICATE`，幂等无操作 |
| 与已完成文件内容冲突的重传 | `QUARANTINED_CONFLICT`，保留原版本，绝不混入 |
| 提交/清理后重放已提交内容 | `IGNORED_COMMITTED`，幂等无操作 |

## 批次完成规则

- `is_ready()` 仅当**所有必需文件**完整且摘要与清单一致时为真；可选文件
  （`required=False`）缺失不阻塞批次，到达则随批次一并冻结。
- `build_batch()` 在就绪后冻结当前完整文件为不可变 `Batch`；重复调用返回
  同一对象（幂等）。已完成文件不会被后续冲突上传篡改，因此批次内容稳定。
- `commit()` 先在日志（journal）下逐文件暂存，**全部暂存完成后才原子翻转
  发布指针**。消费方通过 `snapshot()` 只能看到最近一次完整提交的批次，
  永远看不到提交中途的半份数据。

## 撤销与恢复规则

- **提交中断**：`commit()` 中途失败（可用 `fail_after=N` 模拟）时状态进入
  `COMMITTING`，已暂存文件不会发布，`snapshot()` 保持旧值。必须调用
  `recover()` 回滚暂存内容（journal 记录 `rollback`），状态回到 `IDLE`
  后才可重试提交。
- **撤销未提交批次**：`revoke_batch()` 丢弃已构建但未提交的批次（例如发现
  某文件有问题需重新等待覆盖）。已提交或提交中途的批次不可通过此接口撤销。
- **清理与重放**：提交成功后 `cleanup()` 释放暂存状态，但保留已提交摘要。
  之后重放历史到达事件全部返回 `IGNORED_COMMITTED`，不会重建或覆盖已提交
  批次。未提交前调用 `cleanup()` 会被拒绝。

## 运行测试

```bash
python -m unittest
```
