# 协同事务平台

协同事务平台为需要多机构参与的业务提供统一的机构、授权、案件、证据、审批、资源预约、资金分录、消息归并、通知、定时任务、历史回放，以及数字货币桥**清算窗口与净额结算**能力。项目只使用 Python 标准库与 SQLite，适合在单个 Linux 应用容器内运行。

## 清算窗口与净额结算

`src/civicflow/clearing.py` 在既有机构、授权、不可变资金分录、事件收件箱和恢复任务之上提供：

- **走廊与窗口**：登记走廊（币种对）、参与方与被授权对手方，开设带开启/截止边界的清算窗口，录入并修订窗口汇率快照。
- **限额与流动性**：参与方限额按版本留痕且**按窗口计量**；可用流动性通过不可变的注资/冻结/释放/结算/入账分录占用，跨窗口守恒。
- **指令冻结与净额批次**：桥侧「受理」按合同义务把多笔指令冻结到窗口、锁定流动性并汇入按窗口+币种的可追溯净额批次（含不可变成员关系）。
- **来源序列**：受理、匹配、结算、退回、撤销按 `(来源, 来源键, 序号)` 严格有序推进；序号缺口先缓存，补缺后按序自动推进；迟到事件只进入下一可用窗口；相同序号异文事件独立隔离，相同内容重放返回原批次。
- **暂停与职责分离**：关账前限额变化、汇率修订或合规命中会暂停受影响批次；已关账/已结算批次不可暂停；录入汇率的人不能批准窗口关账。
- **不可抹除的调整**：已结算批次只能用反向分录加新窗口反向义务调整，原批次与原分录保留。
- **可见性**：参与机构只见本方限额/流动性与本方参与的指令，未授权对手方字段以 `***` 遮蔽。
- **可解释与可恢复**：`explain` 解释义务落入的窗口、采用的汇率版本、额度占用与放行人；`checkpoint` 返回最后确认序列、等待缺口与当前流动性占用。

清算演示：

```bash
PYTHONPATH=src python3 -m civicflow.cli --db /tmp/cf-demo.sqlite3 clearing-demo
```

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
