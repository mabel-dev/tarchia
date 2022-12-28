from typing import Optional

from pydantic import BaseModel

class NewBlob(BaseModel):
    location: str
    provider: Optional[str] = None