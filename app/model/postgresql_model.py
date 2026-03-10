from pydantic import BaseModel
from typing import Optional, List, Dict, Any
import uuid


# --------  通用响应模型  -------
class ResponseModel(BaseModel):
    code: Optional[int] = 200
    message: Optional[str] = "success"
    rid: Optional[str] = str(uuid.uuid4())  # 默认值为一个随机的UUID字符串
    data: Any = None

# --------  文档数据模型  -------
class DocumentDataModel(BaseModel):
    identifier_id: str
    file_name: str
    file_url: str
