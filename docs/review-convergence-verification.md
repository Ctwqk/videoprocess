# VP 审查整改与验证记录

日期：2026-09-14。分支：`codex/autoflow-contract-convergence`。

对应附件 `VP审查与收敛建议_697f622.md`。审查基线为 `697f622`，本轮从 `95a2b8f` 开始；期间保留另一项任务快进带来的部署修复，最终集成基线为 `78baa79`。本轮修复已确认的现有实现问题，整理当前设计契约；自主选题、反馈驱动策略和 AI 视频执行仍按路线图推进。

## 修复对应关系

| 报告项 | 落地结果 |
| --- | --- |
| F01 | 统一素材权利判定。资产 ID 不构成许可；`blocked` 优先拒绝；许可、来源、范围、证据随搜索、裁剪和分镜转换保留。Graph 从存储资产读取权利，模型不能自行宣称授权。普通自有实拍素材可用，专用受控生产 profile 保留原有加严约束。 |
| F02 | 移除生产 `_fixture_candidates`。无合适素材返回持久化 `blocked/no_material` 和空图，不能审批执行。演示数据只在测试中显式注入；演示客户端要求真实素材库 UUID。 |
| F03 | 镜头默认必需；素材库正式上传缺必需镜头即停止。预览可呈现部分结果，可选省略记录镜头 ID 和有效目标时长。选材检查最低分、必需与禁止内容证据，不直接采用搜索首项。 |
| F04 | 显式模板、分镜和 graph 模式有确定路由；记录请求模式、实际模式、provider 和回退原因，覆盖模式与来源策略组合。 |
| F05 | Go/Python 共享版本化 `PlanningOptions` 和测试向量，保留显式 `false/0`，拒绝类型、版本和重复字段冲突。模型须同时满足全局与单请求启用条件；规则 graph 不冒充模型结果。 |
| F06 | Python 队列只领取其实现的任务类型，Go 专有任务保持待领取。生产 CLI 启动保护此前已经存在，本轮保留并验证，没有删除兼容 API。 |
| F07 | 生成器不能满足最少镜头数时明确拒绝；截短模板保留结尾，非法 min/max 提前拒绝。 |
| F08 | X/Twitter 使用明确词匹配，删除分镜策略不可达重复分支。 |

复审中还修复了候选编辑与实际分镜来源脱节、嵌套规划参数存储后冲突、编辑丢失规划来源记录的问题。改变素材时必须重新判定权利；执行版本变化使旧批准失效；原有候选锁定/解锁编辑仍可用。混合 graph 中的外部下载来源现在也进入同一候选权利清单，不能因同时包含自有素材而获得内部自动审批。

新增 [当前架构契约入口](current-architecture.md)，为历史总纲、live 规范和分镜计划标明适用范围与覆盖关系，并校正运行手册的迁移版本和 PDS 降级语义。学习影响测试改为实际执行候选选择策略，验证观测性反馈不会改变当前生产排序。

## R01–R13 验收证据

表中的测试文件均位于 `backend/tests/`，Go 和根目录测试另标。

| 编号 | 行为证据 | 结果/边界 |
| --- | --- | --- |
| R01 | `autoflow/test_rights_policy.py`、`test_material_selector.py`、`test_review_convergence.py` | 未知本地许可不能自动上传；内部自动审批不能替代人工审核。 |
| R02 | 同上；graph 存储资产与分镜转换、编辑测试 | blocked 不被候选转换或编辑重写为 allowed。 |
| R03 | `test_review_convergence.py` 的三种来源策略空搜索、SQLite 持久化测试；`test_demo_clients.py` | 无 demo 伪成功，空计划不能审批。 |
| R04 | `test_storyboard_pipeline_builder.py`、`test_review_convergence.py` | 正式上传必需镜头覆盖；预览省略记录；选材最低分和硬约束。 |
| R05 | `test_review_convergence.py` | 默认显式分镜确实进入分镜；24 种模式/来源策略组合报告实际模式。 |
| R06 | `test_channelops_go_autoflow_contract.py`；Go `internal/channelops/handlers_test.go`；`test_review_convergence.py` | 共享请求向量、精确类型和默认值、provider 调用计数与落地记录。使用测试 provider，没有调用收费模型。 |
| R07 | `test_graph_planner.py`、`test_review_convergence.py` | 全局或请求禁用时不调用模型，明确失败/回退，规则结果单独标识。 |
| R08 | `test_storyboard_generator.py` | 六镜头要求超出三镜头模板能力时明确拒绝，不返回违反数量要求的结果。 |
| R09 | `channel_agent/test_models_queue.py` 和现有 runner 启动保护测试 | 不支持类型保持 queued，已有生产启动拒绝规则保留。 |
| R10 | `worker/test_youtube_upload_handler.py`、上传操作存储测试 | 已持久化任务 ID 的可恢复查询超时/断连允许 GET-only 重试，全程一个 POST。未知任务 ID、取消、安全和协议异常保持人工对账边界；旧 uncertain 行不自动迁移。详见运行验证记录。 |
| R11 | `autoflow/test_autoflow_api.py`、`test_review_convergence.py` | 本地批准版本绑定、素材替换、参数存储回读和真正无变化写入通过。PostgreSQL 并发幂等测试因未配置数据库跳过，未声称验证真实竞态。 |
| R12 | 根目录恢复/回滚 Python 测试和 `tests/test_worker_admission_rollback.sh` | 持久状态机与重启回放有界；外部 Docker/SSH 调用不保证整个 shell 脚本墙钟时间有界。详见运行验证记录。 |
| R13 | `autoflow/test_intent_parser.py` | `Explain the process` 不会仅因字母 x 被识别为 X。 |

## 最终检查

使用 Python 3.12 虚拟环境，在最终代码上执行：

| 检查 | 结果 |
| --- | --- |
| 后端 `python3 -m pytest -rs` | **4048 passed, 577 skipped, 62 warnings，84.25 秒** |
| Go `go test ./... -count=1` | 全部通过 |
| 根目录四组恢复/部署事务 Python 测试 | **716 passed, 81 subtests passed，116.67 秒** |
| `bash tests/test_worker_admission_rollback.sh` | **14 项通过，13.040 秒** |
| 最终上传 handler 定向测试 | **61 passed, 47 skipped**，已包含在后端全量中 |
| `git diff --check`、Go 格式检查、新文档相对链接检查 | 通过 |

Ruff 和 mypy 均运行完整检查并与初始分支输出对照：Ruff 11 项存量问题；mypy 61 项、21 个文件的存量问题，规范化行号后没有新增或减少。两项命令返回非零；本轮不顺带修改无关静态检查问题。

独立代码复审发现的来源绑定、规划参数持久化、上传取消竞态及网络异常分类问题均已修复；最终定向复审无未解决的重要问题。

未修改前端，故无需前端构建。未执行真实平台上传、生产部署或线上恢复；PostgreSQL、Redis 和专用确认演练等环境相关测试跳过原因保留在测试输出中。

## 使用与验证边界

- 旧素材缺少权利元数据时会被拦截或要求审核，需要补充真实权利信息。
- 分镜硬约束目前以已有文本证据做确定性匹配，不等于完成语义质量或叙事价值评估；有效时长是已选择镜头的编辑目标时长，不是最终渲染文件的测量值。
- 旧 `uncertain` 上传记录仍需人工对账。新恢复路径仅查询已有任务，不能跨过上传去重边界。
- 本轮保留所有 API、迁移历史、批准版本、幂等、队列租约和外部副作用保护。报告提出的长期产品阶段不是本轮已实现功能。

更完整的 R10–R12 操作边界和命令见 [运行验证记录](review-convergence-runtime-verification.md)。
