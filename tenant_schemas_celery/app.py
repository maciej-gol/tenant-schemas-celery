from __future__ import absolute_import

try:
    import celery
    from celery import Celery
except ImportError:
    raise ImportError("celery is required to use tenant_schemas_celery")

from django.db import connection

from celery.signals import task_prerun, task_postrun


def get_schema_name_from_task(task, kwargs):
    if celery.VERSION[0] < 4:
        # Pop it from the kwargs since tasks don't except the additional kwarg.
        # This change is transparent to the system.
        return kwargs.pop('_schema_name', None)

    # In some cases (like Redis broker) headers are merged with `task.request`.
    task_headers = task.request.headers or task.request
    return task_headers.get('_schema_name')


def switch_schema(task, kwargs, **kw):
    """ Switches schema of the task, before it has been run. """
    # Lazily load needed functions, as they import django model functions which
    # in turn load modules that need settings to be loaded and we can't
    # guarantee this module was loaded when the settings were ready.
    from .compat import get_public_schema_name, get_tenant_model

    old_schema = (connection.schema_name, connection.include_public_schema)
    setattr(task, '_old_schema', old_schema)

    schema = (
        get_schema_name_from_task(task, kwargs) or
        get_public_schema_name()
    )

    # If the schema has not changed, don't do anything.
    if connection.schema_name == schema:
        return

    if connection.schema_name != get_public_schema_name():
        connection.set_schema_to_public()

    if schema == get_public_schema_name():
        return

    tenant = get_tenant_model().objects.get(schema_name=schema)
    connection.set_tenant(tenant, include_public=True)


def restore_schema(task, **kwargs):
    """ Switches the schema back to the one from before running the task. """
    from .compat import get_public_schema_name

    schema_name = get_public_schema_name()
    include_public = True

    if hasattr(task, '_old_schema'):
        schema_name, include_public = task._old_schema

    # If the schema names match, don't do anything.
    if connection.schema_name == schema_name:
        return

    connection.set_schema(schema_name, include_public=include_public)


task_prerun.connect(switch_schema, sender=None,
                    dispatch_uid='tenant_schemas_switch_schema')

task_postrun.connect(restore_schema, sender=None,
                    dispatch_uid='tenant_schemas_restore_schema')


class CeleryApp(Celery):
    def create_task_cls(self):
        return self.subclass_with_self('tenant_schemas_celery.task:TenantTask',
                                       abstract=True, name='TenantTask',
                                       attribute='_app')
