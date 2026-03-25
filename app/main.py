import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# 引入核心响应机制
from app.core.response import UnifiedResponse, configure_exception_handlers
# 引入路由
from app.router.analysis import router as analysis_router
from app.router.file import router as file_router
from app.router.postgresql import router as postgresql_router
from app.router.postgresql_batch import router as postgresql_batch_router

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
    swagger_url = "http://127.0.0.1:8080/docs"
    threading.Timer(1.5, lambda: webbrowser.open(swagger_url)).start()
    uvicorn.run("app.main:app", host="0.0.0.0", port=8080, reload=True)
