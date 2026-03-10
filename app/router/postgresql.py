from fastapi import APIRouter, HTTPException
from app.model.postgresql_model import (ResponseModel, DocumentDataModel)
from app.service.postgresql_service import create_document



router = APIRouter()
@router.post("", response_model=ResponseModel)
async def create_document(document: DocumentDataModel):
    try:
        result = create_document(document.model_dump())

        return ResponseModel(
            data=result
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))