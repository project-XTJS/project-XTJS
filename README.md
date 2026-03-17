# PROJECT-XTJS 项目开发手册

## 1. 项目简介
本项目是一个基于 **FastAPI** 框架构建的基于 **PaddleOCR** 的文字识别到投标文件的合规性自动化分析。系统集成了 **PostgreSQL** 数据库、**MinIO** 对象存储以及 **Celery** 异步任务处理。
---

## 2. 后端业务任务分工表
所有分析校验逻辑统一在 `app/service/analysis_service.py` 中实现。

| 业务模块 | 核心负责人 | 对应代码位置 | 重点开发内容 |
| :--- | :--- | :--- | :--- |
| **OCR 识别调优** | 高海斌 | `ocr_service.py` | 优化模型加载、GPU/CPU 切换逻辑、提高识别精度。 |
| **偏离条款合规性检查** | 高海斌 | `analysis_service.py` | 实现招标文件与投标文件之间的偏离项自动比对算法。 |
| **投标文件完整性审查** | 虞光勇、陶明宇 | `analysis_service.py` | 校验必备章节（如报价函、授权书等）是否缺失。 |
| **格式模板一致性检查** | 虞光勇、陶明宇 | `text_utils.py` | 校验文档字体、段落间距等排版是否符合模板规范。 |
| **开标一览表报价合理性** | 曾俊、滑鹏鹏 | `analysis_service.py` | 提取报价并校验逻辑（如大小写匹配、报价范围预警）。 |
| **分项报价表检查** | 江宇 | `analysis_service.py` | 解析表格，校验分项合计数与总价的逻辑关系。 |
| **签字盖章合规性检查** | 镇昊天、张化飞 | `ocr_service.py` | 调用印章检测模型（Seal Detection）验证盖章状态。 |
| **日期合规性检查** | 镇昊天、张化飞 | `analysis_service.py` | 自动提取签署日期并校验其是否在有效标期内。 |
---

## 3. 目录结构说明
```text
PROJECT-XTJS/
├── app/
│   ├── core/           # 框架核心：统一响应 (Response)、异常拦截
│   ├── schemas/        # 数据契约：入参/出参校验模型 (Pydantic Schemas)
│   ├── router/         # 接口定义：API 路由分发 (analysis, file, postgresql)
│   ├── service/        # 业务逻辑：核心业务 Service 层实现
│   ├── tasks/          # 异步任务：Celery 任务定义与分发
│   ├── utils/          # 工具链：通用文本处理、PDF/Word 解析工具
│   └── main.py         # 应用入口：FastAPI 实例初始化与中间件配置
├── db/migration/       # 数据库：SQL 迁移脚本版本控制
├── run.py              # 快捷启动：本地一键运行脚本（带 Swagger 自动跳转）
└── requirements.txt    # 依赖管理：项目所需 Python 第三方库清单

## 4. 开发指引
安装依赖：
请在虚拟环境下执行依赖安装：
pip install -r requirements.txt
本地启动：
执行启动脚本，系统将自动配置环境并启动服务：
python run.py
服务启动后将自动打开 http://127.0.0.1:8080/docs 查看交互式 API 文档。

开发规范：
Router 层：仅负责参数接收。
Service 层：所有业务校验、计算、数据库交互封装在 Service 类中。
配置管理：统一使用 app/config/settings.py。

## 5. 部署流程
同步代码：
git pull
获取 Commit ID：
git log --oneline
执行部署：
make commit_id=<commit_id>