# sbw-coverer — SBW 控制面分片传感 + 执行器

共享带宽池(SBW)控制面拆分后的**分片**部件(见 `sbw-contract/docs/DESIGN-server-coverer-split.md`)。

- 对**自己覆盖的 K 条 edge**:内嵌 GoBGP **RIB-tap**(判活 / 多跳 BFD / PeerDown)、guard,向自己覆盖的 agent 推 desired-state。
- **只连 sbw-server**(`rpc.ServerCoverer` client):`Watch` 收覆盖分配 + 每边 desired-state、`Report` 上报判死票 / member→edge / agent 注册心跳。
- **绝不碰 YugabyteDB / etcd**;无状态(覆盖与 desired-state 都从 server re-sync)→ 随边数水平扩。

## 状态
**已上线(§8 拆分完成)**:coverer 侧包(`ribtap`/`shard`/`coverage`/`liveness`/`guard`/`deathvote`/`ribevent`/`grpcsrv`)已从单体迁入(含 `gobgp` 依赖,`replace => ../gobgp`),CI 全绿,并在 k3s lab 端到端验证(K=2 coverer 分配/homing + failover + 恢复再平衡)。单体 `sbw-controller` 已退役(仓库归档只读)。共享契约/模型在 `sbw-contract`;全局脑半在 `sbw-server`。

```bash
go build ./...        # 编译
go run ./cmd/sbw-coverer --version
```
