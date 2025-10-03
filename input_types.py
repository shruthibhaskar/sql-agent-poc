from pydantic import BaseModel
from typing import List


class API_Params(BaseModel):
    query: str
    key: str
    user_id: str
    roles: str
    session_id: str
    correlation_id: str = ""
    required_files: List[str] = []
    selections: List[str] = []
    metadata: str = ""
