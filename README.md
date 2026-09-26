# 建立村镇测绘任务断点恢复基础平台

本项目是一套可离线运行的 Python 服务端平台，供县、乡镇和村级工作人员管理新型城镇化安置、土地资源分配、危房安全勘察与改造复核。账号登录、角色权限、业务状态、幂等结果和审计事件保存在 SQLite 中，适合安置经办、自然资源、住建复核与审计人员在单个 Linux 应用容器内协作。

## 目录

- `src/rural_allocation/`：乡镇片区、地块资源池、土地批次、家庭申请、分配运行与移交情景；
- `src/housing_safety/`：危房勘察协议、测量导入、异常复核、分析任务租约和安全结论；
- `src/remediation_review/`：改造案件、现场测量、风险分析、账号登录与质量审批；
- `src/survey_checkout/`：自然资源测绘校核的可恢复分片执行（地块边界比对、面积统计、分片依赖、租约领取与重启重建）；
- `fixtures/`：离线验收使用的勘察协议、结构化测点与测绘校核清单；
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
PYTHONPATH=src python3 -m survey_checkout.acceptance --workspace .
```

四条命令使用临时 SQLite 数据库完成村镇与地块登记、家庭申请分配、危房测量分析、改造审批和测绘校核分片执行，不访问外部网络。

## 测绘校核分片执行

`survey_checkout` 把一次测绘校核任务拆成有依赖关系的分片，服务重启后无需从头计算：

- 任务创建时保存输入清单摘要（`manifest_sha256`）、规则版本摘要（`rule_set_sha256`）、分片依赖图和每个分片的输入快照摘要（`input_sha256`），成果登记时同时保存输出校验值（`output_sha256`）；
- 工作进程通过 `POST /shards/claim` 凭有期限租约领取分片，租约过期后其他进程可接管（`lease_epoch` 递增），完成登记前校验租约归属、输入摘要与规则摘要，迟到结果返回冲突、不能覆盖新持有者；
- 成功登记与后续分片解锁、任务收尾在同一事务内完成，解锁来源记录为 `dependency_unlocked`；
- 失败按任务重试策略（`max_attempts` + `backoff_seconds`）退避重试，次数用尽进入人工处置（`manual`），复核员可决定重新入队（`retry`）或转实地裁决（`skip`）；
- 取消任务只阻止新的领取，已完成分片的成果与审计事件全部保留，在飞分片仍可登记证据但不再解锁后续分片；
- `GET /jobs/{job_id}` 完全由 SQLite 持久行重建各分片状态，并用 `state_source` 说明来源（`initial`、`dependency_pending`、`dependency_unlocked`、`lease_active`、`lease_expired`、`output_recorded`、`retry_exhausted`、`manual_resolution`）。

## HTTP 服务

```bash
PYTHONPATH=src python3 -m rural_allocation.api --database rural.sqlite3 --host 127.0.0.1 --port 8080
PYTHONPATH=src python3 -m housing_safety.api --database housing.sqlite3 --host 127.0.0.1 --port 8081
PYTHONPATH=src python3 -m remediation_review.api --database remediation.sqlite3 --host 127.0.0.1 --port 8082
PYTHONPATH=src python3 -m survey_checkout.api --database survey.sqlite3 --host 127.0.0.1 --port 8083
```

服务均提供 `GET /health`，其余接口使用 JSON。账号登录和角色权限由服务端校验，进程重启后可以继续查询 SQLite 中的业务状态与审计历史。
