from __future__ import absolute_import

try:
    from celery import Celery
except ImportError:
    raise ImportError("celery is required to use tenant_schemas_celery")

from django.db import connection

from celery.signals import task_prerun


def switch_schema(kwargs, **kw):
    """ Switches schema of the task, before it has been run. """
    # Lazily load needed functions, as they import django model functions which
    # in turn load modules that need settings to be loaded and we can't
    # guarantee this module was loaded when the settings were ready.
    from tenant_schemas.utils import get_public_schema_name, get_tenant_model
    # Pop it from the kwargs since tasks don't except the additional kwarg.
    # This change is transparent to the system.
    schema = kwargs.pop('_schema_name', get_public_schema_name())
    connection.set_schema_to_public()
    tenant = get_tenant_model().objects.get(schema_name=schema)
    connection.set_tenant(tenant, include_public=True)

task_prerun.connect(switch_schema, sender=None,
                    dispatch_uid='tenant_schemas_switch_schema')


class CeleryApp(Celery):
    def create_task_cls(self):
        return self.subclass_with_self('tenant_schemas_celery.task:TenantTask',
                                       abstract=True, name='TenantTask',
                                       attribute='_app')
