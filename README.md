# XTJS —— 招投标文件智能审查平台

基于 FastAPI、PaddleOCR、PostgreSQL、MinIO 构建的综合性招投标文件自动化审查系统，提供文档解析、内容查重、商务标形式审查、报价合理性分析、完整性校验、签章日期验证等多种能力。

---

## 技术栈

- **后端框架**：FastAPI + Uvicorn
- **OCR引擎**： PaddleOCR‑VL（PaddleX）
- **数据库**：  PostgreSQL（psycopg2 连接池）
- **对象存储**：MinIO（文件上传/预签名 URL）
- **异步任务**：Celery + Redis（可选）
- **其他依赖**：PyMuPDF、Pillow、SequenceMatcher、pycorrector/MacBERT、nvidia‑smi 等

---

## 部署流程

- python -m venv venv
- ./venv/Scripts/activate
- pip install -r requirements.txt
- 错别字检查使用 pycorrector 的 MacBERT 中文纠错模型，首次运行会按配置加载 `TYPO_MACBERT_MODEL_NAME`，默认通过 `TYPO_MACBERT_DEVICE=cuda` 使用 GPU；当前默认 `TYPO_CHECK_VISIBLE=false`，暂不在展示结果和导出报告中显示
- 需要手动安装对应的cuda版本 pip install paddlepaddle-gpu==3.3.0 -i https://www.paddlepaddle.org.cn/packages/stable/cu130/ 
- python run.py

---

## 用户登录与权限

系统启用了基于 JWT 的登录认证，账号分四级权限：1-普通用户、2-中级用户、3-高级用户、4-管理员；账号统一由管理员创建。

部署前需配置（参考 `.env.example`，复制为 `.env`）：

- `JWT_SECRET_KEY`：JWT 签名密钥，**生产环境务必改为足够长的随机串**（如 `openssl rand -hex 32`），不要沿用默认值。
- `AUTH_INITIAL_ADMIN_USERNAME` / `AUTH_INITIAL_ADMIN_PASSWORD`：首次启动会据此自动创建初始管理员（密码留空则不创建）。创建后请尽快登录修改密码。
- 可选项：`JWT_ACCESS_TOKEN_EXPIRE_MINUTES`（令牌有效期）、`AUTH_MAX_FAILED_ATTEMPTS` / `AUTH_LOCK_MINUTES`（登录失败锁定）、`AUTH_PASSWORD_MIN_LENGTH`（密码最小长度）。

数据库：用户表由 Flyway 迁移 `db/migration/V20260617120000__create_xtjs_users.sql` 自动创建（`make run` 时 flyway 容器会自动执行；本地开发需自行执行迁移）。

接口：登录 `POST /api/auth/login`、当前用户 `GET /api/auth/me`、改密 `POST /api/auth/change-password`；管理员用户管理位于 `/api/auth/users`。除 `/health`、`/`、登录接口外的业务接口均需携带 `Authorization: Bearer <token>`。
