# 建立村镇测绘任务断点恢复基础平台

本项目是一套可离线运行的 Python 服务端平台，供县、乡镇和村级工作人员管理新型城镇化安置、土地资源分配、危房安全勘察与改造复核。账号登录、角色权限、业务状态、幂等结果和审计事件保存在 SQLite 中，适合安置经办、自然资源、住建复核与审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/rural_allocation/`：乡镇片区、地块资源池、土地批次、家庭申请、分配运行与移交情景；
- `src/housing_safety/`：危房勘察协议、测量导入、异常复核、分析任务租约和安全结论；
- `src/remediation_review/`：改造案件、现场测量、风险分析、账号登录与质量审批；
- `src/survey_check/`：自然资源测绘校核的可恢复分片执行，含地块边界比对、面积统计、分片依赖、租约调度与断点续算；
- `fixtures/`：离线验收使用的勘察协议和结构化测点；
- `tests/`：核心规则、权限、错误边界、事务、API 和命令行验收测试。

## 环境

- Linux
- Python 3.11 或更高版本
- 运行时仅使用 Python 标准库和 SQLite

## 测试

```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```

## 构建检查

```bash
python3 -m compileall -q src tests
```

## 离线验收

```bash
PYTHONPATH=src python3 -m rural_allocation.acceptance --workspace .
PYTHONPATH=src python3 -m housing_safety.acceptance --workspace .
PYTHONPATH=src python3 -m remediation_review.acceptance
PYTHONPATH=src python3 -m survey_check.acceptance
```

前三个命令使用临时 SQLite 数据库完成村镇与地块登记、家庭申请分配、危房测量分析和改造审批；测绘校核验收在同一数据库上演练分片领取、租约过期接管、输入变更作废在途结果、人工处置、进程重启状态重建和取消保留证据，不访问外部网络。

## 测绘校核分片执行

测绘校核任务把输入清单摘要、规则版本、分片依赖和输出校验值保存在 SQLite 中：

- 工作进程通过有期限租约领取分片，完成时服务重新计算输入摘要，验证清单与规则未变，并在同一事务内原子解锁后续分片；
- 租约过期可被其他进程接管，围栏令牌保证迟到结果不能覆盖新持有者；
- 失败按退避策略重试，次数用尽进入人工处置（重试或跳过），跳过分片视为已满足并解锁下游；
- 取消任务只阻止新领取，已完成分片的成果与审计证据保留；
- 状态查询从 SQLite 重建每个分片的有效状态并注明来源（租约列、重试计划、依赖表、成果行或人工处置），进程重启后结果一致。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m rural_allocation.api --database rural.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m housing_safety.api --database housing.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m remediation_review.api --database remediation.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m survey_check.api --database survey.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。账号登录和角色权限由服务端校验，进程重启后可以继续查询 SQLite 中的业务状态与审计历史。
