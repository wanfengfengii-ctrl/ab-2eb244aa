# 海洋观测站 · 浮标寄存器可追溯快照网关

从**可能在读取途中更新配置的浮标控制器**取得可追溯寄存器快照，保证每一份证据
都来自**单一控制器修订版**，绝不把两个修订版的数据拼成一份校准证据。

* Python 3.11 标准库实现，零第三方运行时依赖，镜像可离线构建。
* `Dockerfile` + `docker-compose.yml` 一键启动网关、可控浮标模拟器与一次性
  验证服务，三者均含健康检查（verify 本身等待健康检查通过后才开始）。

## 架构

```
POST /api/snapshots                 生成/回放快照
GET  /api/snapshots/{id}            回放完整快照（冲突/不存在 -> 404）
GET  /health                        健康检查

gateway/app.py        HTTP 编排、参数校验、同参去重锁、原子提交
gateway/reader.py     分页读取：首页建立修订、后续页带修订条件、三轮重试、SHA-256
gateway/storage.py    崩溃安全存储（临时文件 + fsync + 原子 rename）
simulator/app.py      可控浮标控制器：每页 <=64 寄存器 + 修订号 + 故障注入
verify/run.py         一次性验证：构建检查 + 单元测试 + 端到端冒烟，退出码汇总
tests/                21 个单元/集成测试与共享冒烟套件
```

### 一致性协议

1. 请求窗口 1..512 个 16 位寄存器，控制器每页最多返回 64 个。
2. 首页**不带**修订条件，响应建立本轮修订号 `R`。
3. 后续页均携带 `?revision=R`；控制器在配置变更后以 `409 revision_mismatch`
   拒绝条件读取。
4. 出现以下任何情况，**丢弃本轮全部数据**并从首页重新开始：
   修订变化 / 条件失败 / HTTP 错误 / 超时 / 断连 / 缺页短页 /
   地址不连续（重复地址或空洞）/ 越界值（非 0..65535）。
5. 三轮仍不稳定：快照落为终态 `conflict`，返回 `409` 与三轮的稳定失败原因，
   且**不留下任何可查询结果**（GET 返回 404）。
6. 证据校验和：把有序值序列按每寄存器 **大端 2 字节**拼接后计算 **小写
   SHA-256**。

### 幂等与崩溃恢复

* 同一 `snapshotId` + 相同参数（`deviceId/startAddress/registerCount`）的
  并发请求在网关内按 id 加锁串行化：只形成一份快照，其余请求回放
  （一个 201，其余 200，`replayed=true`，证据完全一致）。
* 同 id 不同参数：`409 parameter_conflict`。
* 快照仅以原子方式落盘（临时文件 → fsync → `rename`）。进程崩溃遗留的
  `pending` 文件可由相同参数的重试收养并完成；重启后已完成的快照直接回放。
* 超时、缺页、重复地址、越界值等任何异常都不会产生 `complete` 记录。

## 启动

```bash
# 网关宿主机端口可配置（默认 8080；模拟器调试端口默认 18080）
GATEWAY_HOST_PORT=9090 docker compose up --build -d gateway simulator

# 一次性验证服务：自行退出，退出码汇总所有检查
docker compose up --build --abort-on-container-exit --exit-code-from verify
# 退出码 0 = 全部通过；位掩码：1 构建、2 单元测试、4 端到端冒烟、8 健康检查
```

验证完成后查看结果：

```bash
docker compose logs verify
```

## API 示例

```bash
curl -sS -X POST http://localhost:8080/api/snapshots \
  -H 'Content-Type: application/json' \
  -d '{"snapshotId":"snap-2026-10-06-01","deviceId":"buoy-001",
       "startAddress":1000,"registerCount":130}'
```

```json
{
  "snapshotId": "snap-2026-10-06-01",
  "deviceId": "buoy-001",
  "startAddress": 1000,
  "registerCount": 130,
  "state": "complete",
  "revision": 1,
  "values": [12345, 6789, "..."],
  "sha256": "9f2c…（64 位小写十六进制）",
  "attempts": 1,
  "replayed": false
}
```

冲突响应（持续换版 / 传输异常，三轮后）：

```json
{
  "error": "snapshot_conflict",
  "state": "conflict",
  "attempts": 3,
  "attemptReasons": [
    "revision_changed: controller rejected revision precondition (409)",
    "revision_changed: ...",
    "revision_changed: ..."
  ],
  "reason": "revision unstable or transport failing"
}
```

## 模拟器故障注入（验证用）

```bash
curl -sS -X POST http://localhost:18080/admin/reset -d '{"revision":1}' \
  -H 'Content-Type: application/json'
curl -sS -X POST http://localhost:18080/admin/mode \
  -d '{"mode":"flap"}' -H 'Content-Type: application/json'
```

模式：`normal`、`flap`（每页后换版，验证三轮冲突）、
`bump-after`（读 N 页后换版一次，验证丢弃+重试后成功）、
`short-page`、`duplicate-addresses`、`out-of-range`、
`page-error`、`timeout`、`drop`。

## 本地无 Docker 运行

```bash
python3 -m unittest tests.test_unit -v          # 单元测试
SIMULATOR_PORT=18080 python3 -m simulator.app & # 模拟器
GATEWAY_PORT=18081 UPSTREAM_URL=http://127.0.0.1:18080 \
  SNAPSHOT_STORE=./data python3 -m gateway.app &
GATEWAY_URL=http://127.0.0.1:18081 \
  SIMULATOR_URL=http://127.0.0.1:18080 python3 -m verify.run
```
