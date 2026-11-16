# 化工批次工况分群

KMeans 聚类的真实业务分析小型项目。完整模拟客户、痛点、上下游、算法公式与一天工作剧本见 [BUSINESS.md](BUSINESS.md)。

## 模块与业务流程
数据接入（JSON/CSV、字段验证、批次指纹）→ 参数化分析（异步 Python worker）→ 结果复核（指标、图形、明细、解释、批准）→ 定时分发（本地收件箱、CSV/JSON、SHA256）；另有业务总览、审计轨迹和整站检查点/停机恢复。
三个贯穿全栈的实体：Dataset 数据批次、Job 分析任务、Report 分发计划；辅助 User、Audit、Delivery、Checkpoint、RestoreOperation。

## 整站检查点与停机恢复
页面第 6 项「检查点与恢复」由负责人操作，覆盖数据批次、分析任务、分发计划与本地归档文件。

- **取点**：先暂停新的业务写入与派发（新建数据批次、分析任务、分发计划及审批/启用/归档一律拒绝），等待在途的 API 写入与 worker 执行收口，再在同一稳定窗口内固定数据库记录与归档文件。记录清单、每个文件的大小与 SHA256、总指纹一并写入检查点目录。
- **预检**：重新核对清单指纹、数据文件、全部归档文件与引用完整性；缺文件、指纹不符或引用断裂都会列出原因。只有负责人可预检和恢复。
- **恢复**：再次校验检查点，任一文件缺失或指纹不符则直接拒绝。恢复期间业务读写与派发全部返回 503；整站 epoch 递增，旧执行迟到的结果被拒收，Redis 队列中的旧消息不会复活已回退的任务。
- **一次性切换**：归档文件与数据库记录都在完整暂存后切换；切换失败自动回滚到旧版并保留原因。中断（进程退出）后重试同一操作会继续该操作号；已提交的切换不会重复执行，因此不会重复出刊。
- **恢复后的分发计划**：一律先暂停并标记「恢复后待确认」，负责人逐条确认后才能重新启用。
- **可查**：页面展示取点进度、预检结果、恢复步骤与进度、失败原因和 epoch；`GET /checkpoints/status` 提供同等数据。

检查点存放在独立的 `analysis-checkpoints` 卷（`CHECKPOINT_DIR`），不与归档文件混放；归档重建使用卷内 `.restore-*` 临时目录，成功后清理。

## 快速启动
```bash
cp .env.example .env
docker compose up -d --build --wait
docker compose exec -T backend python -m scripts.seed_demo
```
访问 http://localhost:18209；后端文档 http://localhost:19209/docs；数据库宿主端口 17709。
`SEED_DEMO=true` 自动幂等 seed；不覆盖用户修改。手动 seed 命令可重复执行。

## 演示账号与权限
|账号|密码|真实权限|
|---|---|---|
|admin@demo.local|Demo123!|接入、执行、批准、分发、暂停、读取与下载|
|analyst@demo.local|Demo123!|接入、执行、读取与下载；不能批准或安排分发|
|viewer@demo.local|Demo123!|只读与下载|

## 技术与本地开发
Python 3.12 / FastAPI / SQLAlchemy / NumPy / SciPy / scikit-learn；MySQL 8.0；Redis 7；Nginx；原生 HTML CSS JavaScript ES modules（用户指定）。后端所有校验、持久化和算法在服务端。无前端打包步骤。
后端：安装 backend/requirements.txt，设置 DATABASE_URL、REDIS_URL、JWT_SECRET、ARTIFACT_DIR 后在 backend/ 执行 `python -m migrations.initial`、`python -m scripts.seed_demo`、`uvicorn src.main:app`，另开 `python -m src.worker`。前端使用带 /api 反向代理的 Nginx，直接打开 index.html 不支持 API。

## 目录与工程约定
backend/src/{models,schemas,services,routes,controllers,middlewares,constants,config,utils}；backend/scripts/seed_demo.py；backend/migrations/initial.py。
frontend/src/{api,stores,types,components/common,hooks,pages,router,utils,constants}。
Role 枚举：admin/analyst/viewer；JobStatus：queued/running/completed/failed。后端 backend/src/constants/enums.py，前端 frontend/src/constants/enums.js；路由权限在 middlewares/auth.py；页面按相同角色控制操作入口。

## 环境变量与 Docker
COMPOSE_PROJECT_NAME=batch-clusters；FRONTEND_PORT=18209；BACKEND_PORT=19209；DB_PORT=17709；DB_PASSWORD、JWT_SECRET 见 .env.example；SEED_DEMO 默认 true；CHECKPOINT_DIR 默认 /checkpoints。共享命名产物卷供后端下载与 worker 写入，检查点另用独立命名卷，数据库/Redis 各持久卷。
恰好五服务均有健康检查。Redis 队列加 SQL 排队状态兜底；worker 按 next_run 生成本地收件箱文件。周期 60 秒至 30 天。分发结果为批准分析的固定快照；若需新数据重新计算并建立新计划。当前不连接邮件或企业聊天。
错误响应、请求编号、Redis 每 IP 每分钟 240 次限流、JWT 和数据库操作审计。演示密钥与密码仅供本机模拟；生产部署前配置独立密钥与账号。

## 数据与验收
所有数据为固定随机种子生成的模拟业务数据，不能作为实际生产决策证据。项目独立算法、输入边界及业务局限详见 BUSINESS.md。测试与截图统一存放 ../../test-artifacts/gb-1010/，源码不含日志/截图/测试报告。
清理本项目演示数据：`docker compose down -v --remove-orphans`（会删除本项目命名卷）。

## License
MIT，见 LICENSE。
