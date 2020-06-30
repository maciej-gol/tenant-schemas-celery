import inspect

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp

from .test_tasks import get_schema_name


def test_get_schema_name_registration(transactional_db):
    # Celery is not able to pick up settings without this
    app = CeleryApp('testapp')
    app.config_from_object('django.conf:settings', namespace="CELERY")
    
    assert inspect.isclass(get_schema_name)
    assert not issubclass(get_schema_name, TenantTask)
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert task.__class__.__name__ == 'DynamicTenantTask'
    assert isinstance(task, TenantTask)
    assert isinstance(task, get_schema_name)