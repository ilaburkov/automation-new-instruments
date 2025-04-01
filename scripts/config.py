import json
import tempfile
import typing
from typing import Any, List, Optional, Type, TypeVar

from pydantic import BaseModel, TypeAdapter

ImplBaseModel = TypeVar("ImplBaseModel", bound=BaseModel)


def clean_nones(value: Any) -> Any:
    if isinstance(value, list):
        return [clean_nones(x) for x in typing.cast(List[Any], value) if x is not None]
    elif isinstance(value, dict):
        return {
            key: clean_nones(val)
            for key, val in typing.cast(dict[str, Any], value).items()
            if val is not None
        }
    else:
        return value


def as_dict(v: ImplBaseModel | list[ImplBaseModel]) -> Any:
    if isinstance(v, list):
        inner = typing.get_args(list[ImplBaseModel])[0]
        adapter = TypeAdapter(list[inner])
        return clean_nones(adapter.dump_python(v))
    else:
        return clean_nones(v.model_dump(mode="json"))


def from_dict(v: Any | list[Any], t: type[ImplBaseModel]) -> Any:
    if isinstance(v, list):
        adapter = TypeAdapter(list[t])
        return adapter.validate_python(v)
    else:
        return t.model_validate(v)

def dump(
    value: ImplBaseModel | list[ImplBaseModel], w: Any, indent: Optional[int] = None
) -> None:
    json.dump(as_dict(value), w, indent=indent)


def load(r: Any, t: Type[ImplBaseModel]) -> Any:
    data = json.load(r)
    return from_dict(data, t)

