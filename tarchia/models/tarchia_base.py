import orjson
from pydantic import BaseModel


class TarchiaBaseModel(BaseModel):
    def as_dict(self) -> dict:
        """Convert the model object to a dict"""
        from tarchia.utils.serde import to_dict

        return to_dict(self)

    def serialize(self) -> bytes:
        """Convert the model object to a JSON byte string"""
        return orjson.dumps(self.as_dict())
