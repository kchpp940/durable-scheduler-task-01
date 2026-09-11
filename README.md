# Durable Scheduler

单机多进程持久化任务调度系统，纯 Go 标准库实现，无第三方依赖。

## 架构

- `cmd/scheduler`：调度器进程。HTTP API，任务状态机 + 租约 + fencing token，状态通过 append-only 日志（每条记录带 CRC、逐条 fsync）+ 原子快照持久化到数据目录。
- `cmd/worker`：工作者进程。轮询领取任务，持有租约期间后台续租，完成后提交结果。

### 正确性设计

- 状态机：`PENDING -> LEASED -> COMPLETED`；租约到期可被重新领取。
- 每次领取发放单调递增的 fencing token；续租/完成必须携带当前 token，旧执行实例的迟到请求一律 409，不能覆盖新结果。
- 幂等：创建按 `idempotency_key` 去重；相同 token+相同结果的完成是幂等成功，冲突结果拒绝；已完成任务永远不会重新变为可执行。
- 持久化：所有变更先写日志并 fsync 再应答。重启后恢复快照 + 重放日志。日志尾部的不完整记录（撕裂写）被截断；已提交历史或快照损坏（CRC 不匹配）会拒绝启动，绝不静默忽略。
- 压缩：日志超过阈值时写快照（tmp + fsync + rename + dir fsync），再原子重置日志；压缩中途崩溃不破坏最后一个可恢复状态。

## 构建

```sh
go build -o scheduler ./cmd/scheduler
go build -o worker ./cmd/worker
```

## 运行

```sh
./scheduler -addr 127.0.0.1:8080 -data-dir ./data
./worker -scheduler http://127.0.0.1:8080 -id w1
./worker -scheduler http://127.0.0.1:8080 -id w2   # 可并发多个
```

创建/查询任务：

```sh
curl -X POST localhost:8080/tasks -d '{"idempotency_key":"job-1","payload":"hello"}'
curl localhost:8080/tasks/<id>
```

## 测试

```sh
go test ./...        # 单元 + 集成（真实进程、kill -9 恢复）
go test -race ./...  # 竞态检测
```

覆盖：并发领取唯一胜者、租约失效接管、旧实例迟到提交被拒、scheduler SIGKILL 恢复、worker 中途死亡、重复请求幂等、日志尾部截断、历史损坏拒绝启动、快照/压缩恢复。正确性测试均用轮询等待，不依赖固定时长 sleep。
