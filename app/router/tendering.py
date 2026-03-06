from fastapi import APIRouter, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
# 导入minio配置文件中的工具函数
from app.config.minio import upload_tendering_file, delete_tendering_file

router = APIRouter()

@router.post("/upload-file", summary="上传招投标文件")
async def upload_tendering_file_api(file: UploadFile = File(...)):
    """接收前端上传的招投标文件，调用MinIO工具函数存储"""
    try:
        file_url = upload_tendering_file(file)
        return JSONResponse(
            status_code=200,
            content={
                "code": 0,
                "msg": "文件上传成功",
                "data": {"filename": file.filename, "file_url": file_url}
            }
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"上传失败：{str(e)}")

@router.delete("/delete-file/{object_name}", summary="删除招投标文件")
async def delete_tendering_file_api(object_name: str):
    """删除MinIO中指定的招投标文件"""
    try:
        delete_tendering_file(object_name)
        return JSONResponse(
            status_code=200,
            content={"code": 0, "msg": "文件删除成功"}
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败：{str(e)}")