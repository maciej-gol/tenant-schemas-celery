from datetime import datetime, timedelta
from typing import Generic, TypeVar, Dict, Optional

T = TypeVar("T")


class _CacheEntry(Generic[T]):
    def __init__(self, key: str, value: T, expires_at: datetime) -> None:
        self.key = key
        self.value = value
        self.expires_at = expires_at


class SimpleCache(Generic[T]):
    def __init__(self, storage: Optional[Dict[str, _CacheEntry[T]]] = None) -> None:
        self.__items: Dict[str, _CacheEntry[T]] = storage if storage is not None else {}

    def get(self, key: str, default: T) -> T:
        if key not in self.__items or self.__items[key].expires_at < datetime.utcnow():
            return default

        return self.__items[key].value

    def set(self, key: str, value: T, expire_seconds: int) -> None:
        self.__items[key] = _CacheEntry(
            key=key,
            value=value,
            expires_at=datetime.utcnow() + timedelta(seconds=expire_seconds),
        )
