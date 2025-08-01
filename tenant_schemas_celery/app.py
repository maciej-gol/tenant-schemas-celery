try:
    from celery import Celery
except ImportError:
    raise ImportError("celery is required to use tenant_schemas_celery")

from django.db import connection, connections

from celery.signals import task_prerun, task_postrun

from tenant_schemas_celery.task import headers_with_schema


def get_schema_name_from_task(task, kwargs):
    # In some cases (like Redis broker) headers are merged with `task.request`.
    if task.request.headers and "_schema_name" in task.request.headers:
        return task.request.headers.get("_schema_name")
    return task.request.get("_schema_name")


def switch_schema(task, kwargs, **kw):
    """ Switches schema of the task, before it has been run. """
    # Lazily load needed functions, as they import django model functions which
    # in turn load modules that need settings to be loaded and we can't
    # guarantee this module was loaded when the settings were ready.
    from .compat import get_public_schema_name

    old_schema = (connection.schema_name, connection.include_public_schema)
    setattr(task, "_old_schema", old_schema)

    schema = get_schema_name_from_task(task, kwargs) or get_public_schema_name()

    tenant_databases = task.get_tenant_databases()

    # If the schema has not changed, don't do anything.
    if all(connections[db_name].schema_name == schema for db_name in tenant_databases):
        return

    if connection.schema_name != get_public_schema_name():
        for db_name in tenant_databases:
            connections[db_name].set_schema_to_public()

    if schema == get_public_schema_name():
        return

    tenant = task.get_tenant_for_schema(schema_name=schema)
    for db_name in tenant_databases:
        connections[db_name].set_tenant(tenant, include_public=True)


def restore_schema(task, **kwargs):
    """ Switches the schema back to the one from before running the task. """
    from .compat import get_public_schema_name

    schema_name = get_public_schema_name()
    include_public = True

    tenant_databases = task.get_tenant_databases()

    if hasattr(task, "_old_schema"):
        schema_name, include_public = task._old_schema

    # If the schema names match, don't do anything.
    if all(connections[db_name].schema_name == schema_name for db_name in tenant_databases):
        return


    for db_name in tenant_databases:
        connections[db_name].set_schema(schema_name, include_public=include_public)


task_prerun.connect(
    switch_schema, sender=None, dispatch_uid="tenant_schemas_switch_schema"
)

task_postrun.connect(
    restore_schema, sender=None, dispatch_uid="tenant_schemas_restore_schema"
)


class CeleryApp(Celery):
    registry_cls = 'tenant_schemas_celery.registry:TenantTaskRegistry'
    task_cls = 'tenant_schemas_celery.task:TenantTask'

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("task_cls", self.task_cls)
        super().__init__(*args, **kwargs)

    def create_task_cls(self):
        return self.subclass_with_self(
            self.task_cls,
            abstract=True,
            name="TenantTask",
            attribute="_app",
        )

    def send_task(self, name, args=None, kwargs=None, **options):
        options["headers"] = headers_with_schema(options.get("headers") or {})
        return super().send_task(name, args=args, kwargs=kwargs, **options)
