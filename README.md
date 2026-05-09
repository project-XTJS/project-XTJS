# XTJS —— 招投标文件智能审查平台

基于 FastAPI、PaddleOCR、PostgreSQL、MinIO 构建的综合性招投标文件自动化审查系统，提供文档解析、内容查重、商务标形式审查、报价合理性分析、完整性校验、签章日期验证等多种能力。

---

## 技术栈

- **后端框架**：FastAPI + Uvicorn
- **OCR引擎**： PaddleOCR‑VL（PaddleX）
- **数据库**：  PostgreSQL（psycopg2 连接池）
- **对象存储**：MinIO（文件上传/预签名 URL）
- **异步任务**：Celery + Redis（可选）
- **其他依赖**：PyMuPDF、Pillow、SequenceMatcher、nvidia‑smi 等

---

## 项目结构
PROJECT-XTJS/
├── run.py      # 应用启动入口
├── app/
│ ├── main.py           # FastAPI 应用工厂，中间件、路由注册
│ ├── config/
│ │ └── settings.py             # 全局配置
│ ├── core/
│ │ ├── document_types.py       # 文档类型常量与映射
│ │ └── response.py             # 统一响应模型与异常处理器
│ ├── router/
│ │ ├── analysis.py                 # 文档解析与文本分析接口
│ │ ├── dependencies.py             # FastAPI 依赖注入（服务获取）
│ │ ├── file.py                     # 文件对象操作（MinIO 预签名/删除）
│ │ ├── postgresql.py               # 项目、文档、结果 CRUD 及业务分析接口
│ │ ├── postgresql_batch.py         # 批量上传、识别、项目绑定接口
│ │ └── uploaded_json_support.py    # 上传 JSON 文档的解析与持久化
│ ├── schemas/
│ │ ├── analysis.py                 # 分析请求模型
│ │ ├── postgresql.py               # 数据库相关请求模型
│ │ └── recognition.py              # OCR 识别元数据与响应模型
│ ├── service/
│ │ ├── analysis/
│ │ │ ├── bid_document_review.py        # 投标文件审查（错别字、人员复用）
│ │ │ ├── consistency.py                # 模板一致性比对
│ │ │ ├── deviation.py                  # 偏离条款检查（★星标条款）
│ │ │ ├── duplicate_check.py            # 内容查重（精确+相似度）
│ │ │ ├── duplicate_merge.py            # 查重结果聚类合并
│ │ │ ├── integrity.py                  # 完整性校验（附件/材料清单）
│ │ │ ├── itemized_pricing.py           # 分项报价算术与一致性检查
│ │ │ ├── pricing_reasonableness.py     # 报价合理性（大小写、下浮率、限价）
│ │ │ ├── template_extractor.py         # 招标文件模板/条款提取
│ │ │ ├── unified_business_review.py    # 统一商务标审查服务
│ │ │ ├── verification.py               # 签章、日期、公章校验
│ │ │ └── visualizer.py                 # 审查结果可视化（HTML 报告生成）
│ │ ├── analysis_service.py         # 文本分析服务（OCR + 各检查器）
│ │ ├── document_ingest_service.py  # 文档上传、识别、入库流程
│ │ ├── minio_service.py            # MinIO 对象存储服务封装
│ │ ├── ocr_progress.py             # OCR 进度监控器
│ │ ├── ocr_service.py              # PaddleOCR‑VL 服务封装
│ │ ├── postgresql_service.py       # 数据库 CRUD 服务层
│ │ ├── table_parser.py             # 表格结构解析（HTML/Markdown/文本）
│ │ └── tasks/                      # Celery 异步任务（预留）
│ └── utils/
│   └── text_utils.py               # 文本预处理、分块、临时文件工具


## 部署流程
python -m venv venv
./venv/Scripts/activate
pip install -r requirements.txt
pip install paddlepaddle-gpu==3.3.0 -i https://www.paddlepaddle.org.cn/packages/stable/cu130/ 
python run.py
需要手动选择对应的cuda版本


## 主要接口
方法	                路径	                                                    说明
POST	    /api/analysis/analyze-file	                                    上传文件进行 OCR 文本解析
POST	    /api/analysis/run	                                            统一分析（文本分析或项目服务）
GET/POST	/api/postgresql/projects	                                    项目 CRUD
POST	    /api/postgresql/projects/duplicate-check	                    项目查重（商务/技术）
POST	    /api/postgresql/projects/business-bid-format-review	            商务标形式审查
POST	    /api/postgresql/projects/business-bid-duplicate-check	        商务标内容查重
POST	    /api/postgresql/projects/technical-bid-duplicate-check	        技术标内容查重
POST	    /api/postgresql/projects/personnel-reuse-check	                一人多用检查
POST	    /api/postgresql/projects/typo-check	                            错别字检查
GET	        /api/postgresql/projects/{id}/results	                        查看项目分析结果
GET	        /api/postgresql/projects/{id}/results/{key}	                    查看项目单项分析结果
GET	        /api/postgresql/projects/{id}/merged-results	                查看查重合并结果
GET	        /api/postgresql/projects/{id}/visualization-data	            项目可视化聚合数据
GET	        /api/postgresql/documents	                                    查询文档列表
POST	    /api/postgresql/documents	                                    上传并创建文档
GET	        /api/postgresql/documents/{id}	                                查询文档详情
PUT	        /api/postgresql/documents/{id}	                                更新文档信息
DELETE	    /api/postgresql/documents/{id}	                                删除文档
GET	        /api/postgresql/documents/{id}/source	                        获取文档源文件（重定向至 MinIO）
GET	        /api/postgresql/documents/{id}/preview/pages/{page}	            文档页面预览（base64，支持高亮）
POST	    /api/postgresql/projects/batch/recognize	                    批量上传并 OCR 项目文档
POST	    /api/postgresql/projects/batch/ingest-recognize	                创建项目并分阶段执行 OCR 与审查
POST	    /api/postgresql/projects/{id}/continue-technical-ocr	        继续执行技术标 OCR
POST	    /api/postgresql/projects/business-bid-format-review/upload-json	上传 JSON 并执行商务标形式审查
GET	        /health	                                                        健康检查接口