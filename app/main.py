import os

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# 导入路由
from app.router.document import router as document_router
from app.router.ocr import router as ocr_router
from app.router.business_format import router as business_format_router
from app.router.business_duplication import router as business_duplication_router
from app.router.technical_duplication import router as technical_duplication_router

app = FastAPI(
    title="Document Analyzer API",
    description="API for analyzing PDF and Word documents with OCR support",
    version="1.0.0"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 挂载静态文件
# app.mount("/static", StaticFiles(directory="app/static"), name="static")

# 注册路由
app.include_router(document_router, prefix="/api/documents", tags=["documents"])
app.include_router(ocr_router, prefix="/api/ocr", tags=["ocr"])
app.include_router(business_format_router, prefix="/api/business/format", tags=["business-format"])
app.include_router(business_duplication_router, prefix="/api/business/duplication", tags=["business-duplication"])
app.include_router(technical_duplication_router, prefix="/api/technical/duplication", tags=["technical-duplication"])

# 根路径
@app.get("/")
def read_root():
    return {"message": "Welcome to Document Analyzer API"}

# 健康检查
@app.get("/health")
def health_check():
    return {"status": "healthy"}
