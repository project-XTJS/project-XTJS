import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.openapi.utils import get_openapi

# 引入核心响应机制
from app.core.response import UnifiedResponse, configure_exception_handlers
# 引入路由
from app.router.analysis import router as analysis_router
from app.router.file import router as file_router
from app.router.postgresql import router as postgresql_router
from app.router.postgresql_batch import router as postgresql_batch_router
from app.service.postgresql_service import PostgreSQLService

app = FastAPI(
    title="XTJS API",
    description="Unified API for project, file and text analysis workflows",
    version="1.0.0",
    default_response_class=UnifiedResponse  # 全局统一响应
)

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册异常拦截器
configure_exception_handlers(app)

# 注册路由
app.include_router(analysis_router, prefix="/api/analysis", tags=["analysis"])
app.include_router(file_router, prefix="/api/files", tags=["files"])
app.include_router(postgresql_router, prefix="/api/postgresql", tags=["postgresql"])
app.include_router(postgresql_batch_router, prefix="/api/postgresql", tags=["postgresql"])


def _inject_string_choices(schema: dict, choices: list[str]) -> None:
    if not choices or not isinstance(schema, dict):
        return

    if schema.get("type") == "string":
        schema["enum"] = choices
        schema["default"] = choices[0]
        return

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


def _inject_project_identifier_choices(openapi_schema: dict) -> dict:
    try:
        project_identifiers = PostgreSQLService().list_project_identifiers()
    except Exception:
        project_identifiers = []

    paths = openapi_schema.get("paths", {})
    project_path_prefix = "/api/postgresql/projects"

    for path, path_item in paths.items():
        if not str(path).startswith(project_path_prefix):
            continue

        for method in ("get", "post", "put", "delete", "patch"):
            operation = path_item.get(method)
            if not isinstance(operation, dict):
                continue

            for parameter in operation.get("parameters", []):
                if parameter.get("name") != "identifier_id":
                    continue
                if parameter.get("in") not in {"query", "path"}:
                    continue
                schema = parameter.setdefault("schema", {"type": "string"})
                _inject_string_choices(schema, project_identifiers)

            request_body = operation.get("requestBody") or {}
            content = request_body.get("content") or {}
            for media_type in content.values():
                schema = _resolve_schema_reference(openapi_schema, media_type.get("schema"))
                if not isinstance(schema, dict):
                    continue
                properties = schema.get("properties") or {}
                project_identifier_schema = properties.get("project_identifier")
                if isinstance(project_identifier_schema, dict):
                    _inject_string_choices(project_identifier_schema, project_identifiers)

    return openapi_schema


def custom_openapi():
    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    return _inject_project_identifier_choices(openapi_schema)


app.openapi = custom_openapi

@app.get("/", summary="系统根目录", tags=["system"])
def read_root():
    return "Welcome to XTJS API"

@app.get("/health", summary="健康检查接口", tags=["system"])
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import threading
    import webbrowser
    
    # 启动服务器后自动打开Swagger UI
    host = "127.0.0.1"
    port = 8080
    swagger_url = f"http://{host}:{port}/docs"
    browser_timer = threading.Timer(1.5, lambda: webbrowser.open(swagger_url))
    browser_timer.daemon = True
    browser_timer.start()
    uvicorn.run("app.main:app", host=host, port=port, reload=False)
