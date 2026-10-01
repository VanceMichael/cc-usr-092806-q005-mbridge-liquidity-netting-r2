# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务和历史回放能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 目录

- `src/civicflow/`：领域服务、SQLite 持久化、权限和命令行入口。
- `tests/`：核心流程、边界条件和异常路径测试。
- `examples/`：本地演示输入。

## 配置

通过 `CIVICFLOW_DB` 指定 SQLite 文件路径；不设置时命令行使用当前目录下的 `civicflow.sqlite3`。所有时间使用带时区的 ISO 8601 字符串。

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 编译或构建

```bash
PYTHONPATH=src python3 -m compileall -q src
```

## 使用

初始化数据库并运行离线演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 demo
```

查看当前案件：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-demo.sqlite3 list-cases
```

## 清算窗口与净额结算

`civicflow.clearing.ClearingHouse` 与 `civicflow.netting.BridgeNetting` 在既有机构、授权、审批、不可变分录、事件收件箱和恢复任务之上提供数字货币桥清算能力：

- **走廊与窗口登记**：登记走廊（付款/收款币种、窗口时长、截止提前量）、参与方及被授权对手方名单、参与方限额版本；按窗口边界批量开窗并挂汇率快照，窗口不可重叠。
- **净额批次与流动性冻结**：桥侧指令按合同义务冻结进窗口，先锁定付款方流动性（占用限额），再按指令币种汇总毛应付/毛应收，生成每机构净额头寸和可追溯净额批次；折算金额按窗口汇率记录，仅供对账参考。
- **来源序列推进**：受理、受理确认、匹配、结算、退回、撤销按 `(source, source_key, sequence)` 严格连续推进，断序拒绝；同序同文幂等、同序异文写入 `inbox_conflicts` 隔离；跨日以新序号重发同一义务时直接回到原批次，不二次占用。
- **迟到语义**：超过窗口截止时间的事件进入下一可用窗口；窗口关账后才到达的匹配事件在冻结时滚入下一窗口，绝不挤进已关账批次。
- **暂停三因**：关账前的限额变化、汇率修订或合规命中会把受影响批次置为 `on_hold`（窗口同步挂起），恢复后才可关账。
- **职责分离**：录入/修订汇率的主体不能批准窗口关账；关账批准需匹配当前汇率快照，且窗口内不得有暂停批次。
- **只追加不抹账**：已结算批次只能由**新窗口**的反向调整批次冲销，反向调用 `Ledger.reverse_on` 写反向分录，原批次转为 `adjusted`、原分录保留。
- **最小可见性**：机构只能查看本方限额；批次、条目、头寸按本方机构与授权对手方名单裁剪；`explain_obligation` 只允许解释本方义务。
- **可解释与可恢复**：`explain_obligation` 给出义务落入的窗口、采用的汇率版本、流动性占用与适用限额、放行人和反向调整记录；`recover(source, source_key)` 返回最后确认序列、已受理未冻结指令、已冻结未决批次和未释放占用，服务重启后可继续；`verify()` 校验同一义务没有同时落在两个有效批次。

端到端演示（横琴—澳门走廊、迟到顺延、关账职责分离、反向调整、恢复位置）：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/civicflow-clearing.sqlite3 clearing-demo
```
