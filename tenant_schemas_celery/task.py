from celery import Task
from django.db import connection
from tenant_schemas_celery.cache import SimpleCache


_shared_storage = {}


class SharedTenantCache(SimpleCache):
    def __init__(self):
        super().__init__(storage=_shared_storage)


# DjangoTask requires Celery 5.4. Before then, we can't use it.
try:
    from celery.contrib.django.task import DjangoTask

    BaseTask = DjangoTask
except ImportError:
    BaseTask = Task


class TenantTask(BaseTask):
    abstract = True

    tenant_cache_seconds = None
    tenant_databases = None

    @classmethod
    def get_tenant_databases(cls):
        """Return the databases where the schema should be switched"""
        if cls.tenant_databases is not None:
            return cls.tenant_databases
        if hasattr(cls.app.conf, "task_tenant_databases") is True:
            return cls.app.conf.task_tenant_databases
        return ("default",)

    @classmethod
    def tenant_cache(cls):
        return SharedTenantCache()

    @classmethod
    def get_tenant_for_schema(cls, schema_name):
        from tenant_schemas_celery.compat import get_tenant_model

        missing = object()
        cache = cls.tenant_cache()
        cached_value = cache.get(schema_name, default=missing)
        tenant_cache_seconds = cls.tenant_cache_seconds
        if tenant_cache_seconds is None:  # if not set at task level
            try:  # to get from global setting
                tenant_cache_seconds = int(
                    cls._get_app().conf.task_tenant_cache_seconds
                )
            except AttributeError:
                tenant_cache_seconds = 0  # default

        if cached_value is missing:
            cached_value = get_tenant_model().objects.get(schema_name=schema_name)
            cache.set(schema_name, cached_value, expire_seconds=tenant_cache_seconds)

        return cached_value

    def _update_headers(self, kw):
        kw.setdefault("headers", {})
        kw["headers"].setdefault("_schema_name", connection.schema_name)

    def apply(self, args=None, kwargs=None, *arg, **kw):
        self._update_headers(kw)
        return super().apply(args, kwargs, *arg, **kw)
