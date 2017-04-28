import celery
from celery.app.task import Task
from django.db import connection


class TenantTask(Task):
    """ Custom Task class that injects db schema currently used to the task's
        keywords so that the worker can use the same schema.
    """
    abstract = True

    def _update_headers(self, kw):
        kw['headers'] = kw.get('headers') or {}
        self._add_current_schema(kw['headers'])

    def _add_current_schema(self, kwds):
        kwds['_schema_name'] = kwds.get('_schema_name', connection.schema_name)

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
