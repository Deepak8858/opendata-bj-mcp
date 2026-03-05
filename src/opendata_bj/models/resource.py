from pydantic import BaseModel, Field
from typing import Optional

class Resource(BaseModel):
    id: str
    name: str
    description: Optional[str] = ""
    url: str
    format: str
    mimetype: Optional[str] = None
    size: Optional[int] = None
    created: Optional[str] = None
    last_modified: Optional[str] = None
    state: str = "active"
    position: int = 0
    is_local: bool = False
    package_id: str
