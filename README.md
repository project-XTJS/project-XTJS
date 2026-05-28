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
