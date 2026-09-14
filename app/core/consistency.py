"""Explicit conflicts shared by HTTP routes and transactional service methods."""
from fastapi import HTTPException

class ConsistencyConflict(HTTPException):
    def __init__(self, detail="材料已变更，请刷新项目后重新检查"):
        super().__init__(status_code=409, detail=detail)
