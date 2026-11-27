# 批 2B 施工报告

工作树为 `batch2b`，本地 HEAD 为 `ef2c457`。本次只修改工作树。未提交、推送或部署，未使用生产配置或凭据，未运行 agent 二进制连接面板。

## 五条实现

| 条目 | 实现 |
| --- | --- |
| N1-09 | Tracker 从只读采样生成独立的出站 map 及 IP slice。REST 后台编码和 WS 设备上报各自持有自己的快照，下一次采样或快照构造不会改写已交给发送器的数据。 |
| N1-10 | sing-box 每个实例保留自己的累计计数器。采样聚合当前及仍在排空的实例，旧实例退出时将其最终累计值折入已退役总量一次。Xray 从每个实例独立读取并清零计数，将未读尾段累加到内核对象的总量。关停等待异步 REST 发送退出，在停止前采样，并在停止和退役完成后再采样，随后由同一发送逻辑处理队列。 |
| N1-14 | REST 只在 HTTP 上报成功后推进自己的设备游标。WS 为每个节点保留最新的已编码完整快照，设备通知独立于通用写队列。WS 每个会话只在写成功后推进相应节点的游标，新会话从空游标开始重发完整快照，包含空设备集。 |
| N2-01 | relay 先取得状态路径的独占文件锁，再加载和写入状态。取消上下文贯穿配置与上报请求以及分批循环。关停等待周期上报退出，停止监听并强制收尾后同步收集和落盘，随后撤销旧实例的写入资格并释放文件锁。释放后的旧实例拒绝迟到收据和持久化，接手实例读取同一状态文件并立即重试已恢复的批次。 |
| N3-04 | 节点流量队列按生成顺序持久化完整批次及其原始 ID。写入经过临时文件同步、原子替换和目录同步；确认只移除队首。关停先将 A 和 B 一并落盘，再按序发送，最终发送总时限为 5 秒，余下批次由下一实例恢复。持久化失败时保留内存队列并记录错误，发送前需重新完成落盘。 |

## 协议与兼容性

- 节点 REST 的 `traffic`、`traffic_batch`、`alive`、`online` 和状态字段沿用原载荷定义。WS 沿用 `report.devices` 及机器模式的 `node_id`、`devices` 包装。REST 游标以 HTTP 成功为准，WS 游标以本地 WebSocket 写成功为准。现有 WS 协议没有面板应用回执。
- Relay 继续使用原始 `rule_code`、`boot_id`、`seq`，面板仍以 `machine:rule:boot:seq` 派生批次 ID。重发复用原批次。规则删除及 DRAINING 的成功 ACK 仍在排空或强切完成、旧规则停止转发、末批本地持久化之后发出。
- 节点可读取旧版单批文件，新版保持队首的旧字段并用 `following` 存放后续批次。旧 agent 只读队首，且确认队首后会删除整个文件。带有积压队列的 agent 回退不能依靠旧进程完成这部分队列恢复。
- REST 保留空 `alive` 省略行为，明确的空快照由 WS 发送。若要求纯 REST 实时清空面板设备视图，需要面板定义并验证空 `alive` 的清空语义。2A/2B 四组合矩阵与面板墓碑窗口的真实联验留给隔离环境。

## 文件清单

| 路径 | 文件 |
| --- | --- |
| `internal/tracker/` | `tracker.go`、`tracker_batch4_test.go` |
| `internal/service/` | `service.go`、`service_batch2b_test.go` |
| `internal/controlplane/` | `machine.go`、`panel.go`、`types.go` |
| `internal/panel/` | `client.go`、`relay.go`、`ws.go`、`relay_batch2b_test.go`、`ws_batch2b_test.go` |
| `internal/machine/` | `machine.go` |
| `internal/kernel/` | `kernel.go`、`singbox/conntracker.go`、`singbox/singbox.go`、`singbox/singbox_batch2b_test.go`、`xray/xray.go`、`xray/xray_test.go` |
| `internal/relay/` | `relay.go`、`relay_batch2b_test.go`、`relay_batch1a_test.go`、`relay_batch4_test.go`、`relay_batch5_test.go`、`relay_test.go` |
| 根目录 | `BATCH2B-REPORT.md` |

`go.mod` 和 `go.sum` 无变化。`relay_batch1a_test.go`、`relay_batch4_test.go`、`relay_batch5_test.go`、`relay_test.go` 的原断言保留，测试调用适配了带上下文的 relay 接口。

## 定向测试

| 条目 | 测试 | 最终结果 |
| --- | --- | --- |
| N1-09 | `TestBatch2BAsyncDeviceSnapshotIsIndependent` 并发构造 WS 快照并编码第一份 REST 快照，同时逐一检查快照内容。 | 通过，含 `-race` |
| N1-10 | `TestBatch2BSingBoxTrafficAcrossReplacementAndDrain` 走实例换代及退役路径，验证新计数高于旧值、排空期及退役前尾段字节。`TestBatch2BXrayRetiresIndependentStats` 验证独立 stats 来源。关停用例验证末次 tick 后及 Stop 期间的字节均进入 B。 | 通过，含 `-race` |
| N1-14 | `TestBatch2BFailedRESTDeviceSnapshotRetriesIndependentlyOfWS` 验证 REST 失败后 WS 可发送同内容、REST 可重试。`TestBatch2BDeviceSnapshotsSurviveQueueSaturationAndReconnect` 验证通用队列饱和、写失败、会话游标重置和空快照。 | 通过，含 `-race` |
| N1-14 | `TestBatch2BWSReconnectReplaysFullDeviceSnapshot` 用本地 WS 两次握手读取完整快照。 | 沙箱拒绝创建回环套接字，已跳过 |
| N2-01 | `TestBatch2BRelayHandoffCancelsOldWriterAndPreservesNextGeneration` 启动旧 manager，验证取消在途请求、末次入盘、锁交接、旧实例迟到确认及保存被拒。`TestBatch2BRelayTrafficRequestUsesShutdownContext` 用内存 HTTP transport 验证实际请求的上下文取消。 | 通过，含 `-race` |
| N3-04 | `TestBatch2BShutdownPersistsAndSendsTrafficInOrder` 覆盖 A/B 均确认、A 持续失败、A 确认后 B 失败三种关停结果，验证重载后的顺序、批次 ID、时间窗和字节。`TestBatch2BReadsLegacyBatchBeforeNewWindows` 验证旧单批文件迁移。 | 通过，含 `-race` |

最终定向测试在 tracker、panel、relay、service、sing-box 和 Xray 六包串行执行。下列两轮退出码均为 0，均包含上述回环测试的一次跳过。既有的旧批次恢复、缺失目录、ACK 前持久化失败、relay 持久化后才发送，以及四个 relay 分批确认用例也通过。

- `CGO_ENABLED=1 go test -race -p 1 -count=1 -v <上述六包> -run <定向过滤>`
- `go test -p 1 -count=1 -v -tags 'with_quic with_utls with_wireguard with_acme with_clash_api' <上述六包> -run <定向过滤>`

全量 `go test -p 1 -count=1 ./...` 退出码为 1。controlplane、machine、panel、relay 分别在 `TestPanelControlPlaneInitialRejectsInvalidCustomOutbounds`、`TestRunWaitsForRelayManagerOnShutdown`、`TestGetConfig_Success`、`TestConcurrentFlushesLoseNothing` 遇到 `httptest` 创建回环套接字失败，错误为 `operation not permitted`。其余有测试的包通过，各包跳过项按原有条件执行。测试使用指定的 Go 二进制、并发和缓存目录。原始日志位于 `/root/batch2b-tmp/` 的 `batch2b-targeted-race.log`、`batch2b-tagged.log`、`batch2b-full.log` 和 `batch2b-baseline-udp.log`。

## 存量问题与验证边界

- `TestBuildRuleUDPUsesUDPMaxSessions` 单独运行同样阻塞在创建 TCP 回环套接字，未进入已知 flaky 的 UDP 断言。fork 的 `Router.UpdateRules` / `matchRule` race 与 machine 测试自 race 未在本批定向竞态运行中触发，相关真实转发及 machine socket 测试仍待沙箱外复跑。
- 内核尾段测试使用实例切换与计数器夹具。最终采样以核心关闭完成为边界，沙箱内未验证强切真实 TCP/UDP 时底层零拷贝计数回调的最终时序，也未运行真实 relay 转发下的热重载和规则撤销联验。
- 节点及 relay 的关停恢复依赖状态目录写入成功。本地文件系统拒绝写入或同步时会记录错误，磁盘上的最后可读状态是重启恢复的上限。
