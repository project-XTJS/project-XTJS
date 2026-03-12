import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

load_dotenv()

from app.router.analysis import router as analysis_router
from app.router.file import router as file_router
from app.router.postgresql import router as postgresql_router

app = FastAPI(
    title="XTJS API",
    description="Unified API for project, file and text analysis workflows",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(analysis_router, prefix="/api/analysis", tags=["analysis"])
app.include_router(file_router, prefix="/api/files", tags=["files"])
app.include_router(postgresql_router, prefix="/api/postgresql", tags=["postgresql"])


@app.get("/")
def read_root():
    return {"message": "Welcome to XTJS API"}


@app.get("/health")
def health_check():
    return {"status": "healthy"}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
