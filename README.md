# PROJECT-XTJS 项目开发手册

## 1. 项目简介
本项目基于 **FastAPI** 框架构建和 **PaddleOCR** 的文字识别到投标文件的合规性自动化分析。系统集成了 **PostgreSQL** 数据库、**MinIO** 对象存储以及 **Celery** 异步任务处理。
---

## 2. 业务开发分工
为便于多人协作，将分析逻辑解耦至 `app/service/analysis/` 目录下的独立文件中。

| 业务模块 | 核心负责人 | 对应代码文件 | 任务说明 |
| :--- | :--- | :--- | :--- |
| **OCR 识别调优** | 高海斌 | `app/service/ocr_service.py` | 优化模型加载、GPU/CPU 切换逻辑、提高识别精度。 |
| **偏离条款检查** | 高海斌 | `app/service/analysis/deviation.py` | 实现招标与投标文件之间的条款差异化比对算法。 |
| **完整性审查** | 虞光勇、陶明宇 | `app/service/analysis/integrity.py` | 校验必备章节是否缺失，评估文档完整度。 |
| **格式一致性检查** | 虞光勇、陶明宇 | `app/utils/text_utils.py` | 校验文档字体、段落间距等排版是否符合规范。 |
| **报价合理性检查** | 曾俊、滑鹏鹏 | `app/service/analysis/pricing_reasonableness.py` | 提取总报价，校验大小写匹配及数值合理性。 |
| **分项报价表检查** | 江宇 | `app/service/analysis/itemized_pricing.py` | 解析分项报价表格，校验合计数与总价逻辑。 |
| **签字/盖章/日期** | 镇昊天、张化飞 | `app/service/analysis/verification.py` | 验证印章状态及自动提取校验签署日期。 |

---

## 3. 开发指引
同步代码：
git pull
获取 Commit ID：
git log --oneline
执行部署：
make commit_id=<commit_id>

## 4. 部署流程
本地部署：
python -m venv venv
./venv/Scripts/activate
pip install -r requirements.txt
在初始化环境后，还需要手动安装对应cuda的paddle库才支持gpu版本，查看requirements.txt
python run.py
服务启动后将自动打开 http://127.0.0.1:8080/docs 查看交互式 API 文档。

## 5. 通过API交互测试
运行run.py启动服务，找到 POST /api/analysis/run (统一文本分析接口)，点击右侧的 "Try it out" 按钮。
在 Request body 中修改 JSON 内容：
task_type: 选择任务类型{
    "integrity_check",     # 完整性审查
    "pricing_reason",      # 报价合理性
    "itemized_pricing",    # 分项报价
    "deviation_check",     # 偏离检查
    "full_analysis"        # 全量分析
}
text: 粘贴一段从 PDF 或 Word 中复制的测试文本；
点击蓝色的 "Execute" 按钮，在下方 Responses 区域查看返回的 JSON 结果。如果报错，系统会通过统一响应包装器返回详细的错误信息。

## 6. 业务模块独立自测
为了提高开发效率，无需启动整个项目即可测试自己的 `.py` 代码：
1. **准备测试文件**：在根目录准备一个测试 PDF 或 Word。
2. **修改脚本配置**：打开 `test_modules.py`，将底部的 `SAMPLE_FILE` 修改为你的文件名。
3. **运行测试**：python test_modules.py

## 7. 目录结构说明
PROJECT-XTJS/
├── app/
│   ├── config/           # 全局配置参数目录 (settings.py)
│   ├── core/             # 框架核心：统一响应格式、全局异常拦截
│   ├── router/           # API 路由分发中心
│   │   ├── analysis.py   # 核心分析接口 (前端对接主力)
│   │   ├── dependencies.py # 依赖注入机制
│   │   ├── file.py       # 文件上传与 MinIO 交互
│   │   └── postgresql.py # DB 测试接口
│   ├── schemas/          # 数据契约：Pydantic 请求/响应模型验证
│   ├── service/          # 业务逻辑层
│   │   ├── analysis/     # 各业务子模块独立文件
│   │   ├── analysis_service.py   # 统一调度中心：负责串联各子模块
│   │   ├── minio_service.py      # OSS 对象存储服务
│   │   ├── ocr_service.py        # 底层 OCR 识别与印章定位服务
│   │   └── postgresql_service.py # 数据库交互服务
│   ├── tasks/            # Celery 异步任务定义
│   └── utils/            # 通用工具链 (text_utils.py)
├── db/                   # 数据库 SQL 迁移脚本
├── requirements.txt      # 依赖包列表
└── run.py                # 本地一键启动脚本