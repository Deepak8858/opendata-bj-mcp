from pydantic import BaseModel, Field
from typing import List, Optional, Any
from opendata_bj.models.resource import Resource

class Dataset(BaseModel):
    id: str
    dataset_id: Optional[str] = None
    name: str
    title: str
    description: Optional[str] = ""
    organization: Optional[str] = None
    category: Optional[str] = None
    tags: List[str] = []
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    metadata_created: Optional[str] = None
    metadata_modified: Optional[str] = None
    state: str = "active"
    is_active: bool = True
    is_open: bool = True
    resources: List[Resource] = []
    
    # Extras et métadonnées additionnelles
    num_resources: int = 0
    num_tags: int = 0
    metadata_quality: int = 0
