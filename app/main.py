# -*- coding: utf-8 -*-
"""
FastAPI 应用主入口模块。

创建并配置 FastAPI 应用实例，注册中间件、异常处理器和路由，
并自定义 OpenAPI schema 以注入项目标识可选值。
"""

import uvicorn
from pathlib import Path
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.openapi.utils import get_openapi

# 核心响应机制
from app.core.response import UnifiedResponse, configure_exception_handlers
# 路由模块
from app.router.analysis import router as analysis_router
from app.router.file import router as file_router
from app.router.postgresql import router as postgresql_router
from app.router.postgresql_batch import router as postgresql_batch_router
# 服务层（用于获取项目列表）
from app.service.postgresql_service import PostgreSQLService
from app.core.document_types import (
    DOCUMENT_TYPE_BUSINESS_BID,
    DOCUMENT_TYPE_TECHNICAL_BID,
    DOCUMENT_TYPE_TENDER,
)

app = FastAPI(
    title="XTJS 接口文档",
    description="项目、文件与文本分析统一接口",
    version="1.0.0",
    default_response_class=UnifiedResponse,  # 全局统一响应格式
)

# 跨域中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
# GZip 压缩中间件，超过 1KB 的响应自动压缩
app.add_middleware(GZipMiddleware, minimum_size=1024)

# 注册全局异常拦截器（HTTP 异常、参数校验异常等）
configure_exception_handlers(app)

# 注册路由模块
app.include_router(analysis_router, prefix="/api/analysis", tags=["文档解析"])
app.include_router(file_router, prefix="/api/files", tags=["文件存储"])
app.include_router(postgresql_router, prefix="/api/postgresql", tags=["项目业务"])
app.include_router(postgresql_batch_router, prefix="/api/postgresql", tags=["项目业务"])


# —— OpenAPI 增强：为项目/文档标识字段注入人类可读可选值 ——

HTTP_METHODS = ("get", "post", "put", "delete", "patch")
PROJECT_FIELD_NAMES = {
    "identifier_id",
    "project_identifier",
    "project_identifier_id",
}
DOCUMENT_FIELD_CHOICES = {
    "document_identifier": "all",
    "tender_document_identifier": DOCUMENT_TYPE_TENDER,
    "business_bid_document_identifier": DOCUMENT_TYPE_BUSINESS_BID,
    "technical_bid_document_identifier": DOCUMENT_TYPE_TECHNICAL_BID,
}


def _inject_string_choices(schema: dict, choices: list[str]) -> None:
    """将字符串类型 schema 替换为枚举，并设置默认值。"""
    if not choices or not isinstance(schema, dict):
        return

    if schema.get("type") == "string":
        schema["enum"] = choices
        schema["default"] = choices[0]
        return

    # 处理 anyOf 中包含 string 的可选字段
    any_of = schema.get("anyOf")
    if isinstance(any_of, list):
        string_variant = next(
            (item for item in any_of if isinstance(item, dict) and item.get("type") == "string"),
            None,
        )
        if string_variant is not None:
            schema.pop("anyOf", None)
            schema["type"] = "string"
            schema["nullable"] = True
            schema["enum"] = choices
            schema["default"] = choices[0]


def _resolve_schema_reference(openapi_schema: dict, schema: dict | None) -> dict | None:
    """解析 `$ref` 引用，返回目标 schema 定义。"""
    if not isinstance(schema, dict):
        return None

    reference = schema.get("$ref")
    if reference and reference.startswith("#/components/schemas/"):
        schema_name = reference.rsplit("/", 1)[-1]
        return (
            openapi_schema.get("components", {})
            .get("schemas", {})
            .get(schema_name)
        )

    return schema


def _load_openapi_display_choices() -> tuple[list[str], list[str], dict[str, list[str]]]:
    """从数据库加载 Swagger 下拉展示值。"""
    try:
        db_service = PostgreSQLService()
        project_identifier_choices = db_service.list_project_identifiers()
        project_choices = db_service.list_project_display_choices()
        document_choices = {
            "all": db_service.list_document_display_choices(),
            DOCUMENT_TYPE_TENDER: db_service.list_document_display_choices(DOCUMENT_TYPE_TENDER),
            DOCUMENT_TYPE_BUSINESS_BID: db_service.list_document_display_choices(
                DOCUMENT_TYPE_BUSINESS_BID,
            ),
            DOCUMENT_TYPE_TECHNICAL_BID: db_service.list_document_display_choices(
                DOCUMENT_TYPE_TECHNICAL_BID,
            ),
        }
    except Exception:
        project_identifier_choices = []
        project_choices = []
        document_choices = {
            "all": [],
            DOCUMENT_TYPE_TENDER: [],
            DOCUMENT_TYPE_BUSINESS_BID: [],
            DOCUMENT_TYPE_TECHNICAL_BID: [],
        }
    return project_identifier_choices, project_choices, document_choices


def _parameter_schema(parameter: dict) -> dict:
    """返回参数 schema，必要时补上字符串 schema。"""
    return parameter.setdefault("schema", {"type": "string"})


def _mark_project_parameter(parameter: dict, choices: list[str]) -> None:
    """把参数展示成项目名下拉选择。"""
    schema = _parameter_schema(parameter)
    schema["title"] = "项目名"
    parameter["description"] = (
        "请选择或输入项目名；旧 UUID 仍兼容。Swagger 下拉值会由后端解析为项目 UUID。"
    )
    _inject_string_choices(schema, choices)


def _mark_project_identifier_parameter(parameter: dict, choices: list[str]) -> None:
    """把参数展示成项目 identifier_id（UUID）下拉选择。"""
    schema = _parameter_schema(parameter)
    schema["type"] = "string"
    schema["title"] = "identifier_id"
    schema.pop("nullable", None)
    parameter["description"] = "请选择或输入项目 identifier_id（UUID）。"
    _inject_string_choices(schema, choices)


def _mark_document_parameter(parameter: dict, choices: list[str]) -> None:
    """把参数展示成文件名下拉选择。"""
    schema = _parameter_schema(parameter)
    schema["title"] = "文件名"
    parameter["description"] = (
        "请选择或输入文件名；旧 UUID 仍兼容。文件名重复时下拉值会附带 UUID。"
    )
    _inject_string_choices(schema, choices)


def _inject_body_display_choices(
    openapi_schema: dict,
    operation: dict,
    project_choices: list[str],
    document_choices: dict[str, list[str]],
) -> None:
    """为请求体中的项目/文档标识字段注入人类可读选项。"""
    request_body = operation.get("requestBody") or {}
    content = request_body.get("content") or {}
    for media_type in content.values():
        schema = _resolve_schema_reference(openapi_schema, media_type.get("schema"))
        if not isinstance(schema, dict):
            continue
        properties = schema.get("properties") or {}
        for field_name, field_schema in properties.items():
            if field_name in PROJECT_FIELD_NAMES and isinstance(field_schema, dict):
                _inject_string_choices(field_schema, project_choices)
                field_schema["title"] = "项目名"
                field_schema["description"] = "请选择或输入项目名；旧 UUID 仍兼容。"
            choice_key = DOCUMENT_FIELD_CHOICES.get(field_name)
            if choice_key and isinstance(field_schema, dict):
                _inject_string_choices(field_schema, document_choices.get(choice_key, []))
                field_schema["title"] = "文件名"
                field_schema["description"] = "请选择或输入文件名；旧 UUID 仍兼容。"


def _inject_display_choices(openapi_schema: dict) -> dict:
    """
    在 OpenAPI schema 中将 UUID 参数转换成人类可读的项目名/文件名下拉值。

    注意：这里只改变 Swagger 展示和提交值，后端仍兼容 UUID。
    """
    project_identifier_choices, project_choices, document_choices = _load_openapi_display_choices()

    paths = openapi_schema.get("paths", {})
    rewritten_paths: dict[str, dict] = {}

    for path, path_item in paths.items():
        path_text = str(path)
        is_project_path = "/projects/{identifier_id}" in path_text
        is_document_path = "/documents/{identifier_id}" in path_text
        is_project_result_path = "/results/{project_identifier_id}" in path_text
        rewritten_path = path_text
        if is_project_path:
            rewritten_path = rewritten_path.replace("{identifier_id}", "{project_name}")
        if is_document_path:
            rewritten_path = rewritten_path.replace("{identifier_id}", "{file_name}")
        if is_project_result_path:
            rewritten_path = rewritten_path.replace(
                "{project_identifier_id}",
                "{project_name}",
            )

        for method in HTTP_METHODS:
            operation = path_item.get(method)
            if not isinstance(operation, dict):
                continue

            # 处理路径/查询参数中的项目和文档标识
            for parameter in operation.get("parameters", []):
                parameter_name = parameter.get("name")
                parameter_location = parameter.get("in")
                if parameter_location not in {"query", "path"}:
                    continue

                if parameter_location == "path" and is_project_path and parameter_name == "identifier_id":
                    parameter["name"] = "project_name"
                    _mark_project_parameter(parameter, project_choices)
                    continue

                if parameter_location == "path" and is_document_path and parameter_name == "identifier_id":
                    parameter["name"] = "file_name"
                    _mark_document_parameter(parameter, document_choices.get("all", []))
                    continue

                if (
                    parameter_location == "path"
                    and is_project_result_path
                    and parameter_name == "project_identifier_id"
                ):
                    parameter["name"] = "project_name"
                    _mark_project_parameter(parameter, project_choices)
                    continue

                if parameter_location == "query" and parameter_name in PROJECT_FIELD_NAMES:
                    _mark_project_parameter(parameter, project_choices)

            _inject_body_display_choices(
                openapi_schema,
                operation,
                project_choices,
                document_choices,
            )

        rewritten_paths[rewritten_path] = path_item

    openapi_schema["paths"] = rewritten_paths
    return openapi_schema


def custom_openapi():
    """自定义 OpenAPI 生成函数，注入项目名/文件名可选值。"""
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    return _inject_display_choices(openapi_schema)


app.openapi = custom_openapi


# —— 系统状态接口 ——


@app.get("/", summary="系统根目录", tags=["系统"])
def read_root():
    return "Welcome to XTJS API"


@app.get("/health", summary="健康检查接口", tags=["系统"])
def health_check():
    return {"status": "healthy"}


# —— 开发模式直接启动 ——

if __name__ == "__main__":
    import threading
    import webbrowser

    host = "127.0.0.1"
    port = 8080
    swagger_url = f"http://{host}:{port}/docs"
    # 启动服务器后自动打开 Swagger UI
    browser_timer = threading.Timer(1.5, lambda: webbrowser.open(swagger_url))
    browser_timer.daemon = True
    browser_timer.start()
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        reload=True,
        reload_dirs=[str(Path(__file__).resolve().parents[1])],
    )
