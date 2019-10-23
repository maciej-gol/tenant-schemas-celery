import celery
from celery.app.task import Task
from django.db import connection

from tenant_schemas_celery.cache import SimpleCache

_shared_storage = {}


class SharedTenantCache(SimpleCache):
    def __init__(self):
        super(SharedTenantCache, self).__init__(storage=_shared_storage)


class TenantTask(Task):
    """ Custom Task class that injects db schema currently used to the task's
        keywords so that the worker can use the same schema.
    """

    abstract = True

    tenant_cache_seconds = 0

    @classmethod
    def tenant_cache(cls):
        return SharedTenantCache()

    @classmethod
    def get_tenant_for_schema(cls, schema_name):
        from .compat import get_tenant_model

        missing = object()
        cache = cls.tenant_cache()
        cached_value = cache.get(schema_name, default=missing)

        if cached_value is missing:
            cached_value = get_tenant_model().objects.get(schema_name=schema_name)
            cache.set(
                schema_name, cached_value, expire_seconds=cls.tenant_cache_seconds
            )

        return cached_value

    def _update_headers(self, kw):
        kw["headers"] = kw.get("headers") or {}
        self._add_current_schema(kw["headers"])

    def _add_current_schema(self, kwds):
        kwds["_schema_name"] = kwds.get("_schema_name", connection.schema_name)

    def apply_async(self, args=None, kwargs=None, *arg, **kw):
        if celery.VERSION[0] < 4:
            kwargs = kwargs or {}
            self._add_current_schema(kwargs)

        else:
            # Celery 4.0 introduced strong typing and the `headers` meta dict.
            self._update_headers(kw)
        return super(TenantTask, self).apply_async(args, kwargs, *arg, **kw)

    def apply(self, args=None, kwargs=None, *arg, **kw):
        if celery.VERSION[0] < 4:
            kwargs = kwargs or {}
            self._add_current_schema(kwargs)

        else:
            # Celery 4.0 introduced strong typing and the `headers` meta dict.
            self._update_headers(kw)
        return super(TenantTask, self).apply(args, kwargs, *arg, **kw)
