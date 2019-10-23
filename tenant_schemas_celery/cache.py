from datetime import datetime, timedelta


class _CacheEntry(object):
    def __init__(self, key, value, expires_at):
        self.key = key
        self.value = value
        self.expires_at = expires_at


class SimpleCache(object):
    def __init__(self, storage=None):
        self.__items = storage if storage is not None else {}

    def get(self, key, default):
        if key not in self.__items or self.__items[key].expires_at < datetime.utcnow():
            return default

        return self.__items[key].value

    def set(self, key, value, expire_seconds):
        self.__items[key] = _CacheEntry(
            key=key,
            value=value,
            expires_at=datetime.utcnow() + timedelta(seconds=expire_seconds),
        )
