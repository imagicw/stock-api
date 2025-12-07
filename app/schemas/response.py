from typing import Generic, TypeVar, Optional
from pydantic import BaseModel

T = TypeVar("T")

class Response(BaseModel, Generic[T]):
    code: int = 0
    msg: str = "success"
    data: Optional[T] = None
    total: Optional[int] = None

    @classmethod
    def success(cls, data: T = None, msg: str = "success", total: int = None):
        return cls(code=0, msg=msg, data=data, total=total)

    @classmethod
    def error(cls, code: int = -1, msg: str = "error", data: T = None):
        return cls(code=code, msg=msg, data=data)
