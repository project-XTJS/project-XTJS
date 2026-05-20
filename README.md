# XTJS —— 招投标文件智能审查平台

基于 FastAPI、PaddleOCR、PostgreSQL、MinIO 构建的综合性招投标文件自动化审查系统，提供文档解析、内容查重、商务标形式审查、报价合理性分析、完整性校验、签章日期验证等多种能力。

---

## 技术栈

- **后端框架**：FastAPI + Uvicorn
- **OCR引擎**： PaddleOCR‑VL（PaddleX）
- **数据库**：  PostgreSQL（psycopg2 连接池）
- **对象存储**：MinIO（文件上传/预签名 URL）
- **异步任务**：Celery + Redis（可选）
- **其他依赖**：PyMuPDF、Pillow、SequenceMatcher、LanguageTool、OpenJDK 17、nvidia‑smi 等

---

## 部署流程

- python -m venv venv
- ./venv/Scripts/activate
- pip install -r requirements.txt
- 如需启用 LanguageTool 错别字检查，需安装 Java 运行时（推荐 OpenJDK 17）；Docker 镜像会自动安装 openjdk-17-jre-headless
- 需要手动安装对应的cuda版本 pip install paddlepaddle-gpu==3.3.0 -i https://www.paddlepaddle.org.cn/packages/stable/cu130/ 
- python run.py
