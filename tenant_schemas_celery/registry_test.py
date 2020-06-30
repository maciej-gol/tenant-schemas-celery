import inspect

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp

from .test_app import app
from .test_tasks import get_schema_name


def test_get_schema_name_registration(transactional_db):
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name in app._tasks
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name in app._tasks
    
    task = app._tasks[name]
    assert task.__class__.__name__ == 'DynamicTenantTask'
    assert isinstance(task, TenantTask)
    assert isinstance(task, get_schema_name)
