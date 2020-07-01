import inspect

import pytest
from celery import Task
from celery.task import Task as LegacyTask

from tenant_schemas_celery.task import TenantTask
from tenant_schemas_celery.app import CeleryApp

from .test_app import app
from .test_tasks import get_schema_name, get_schema_from_class_task, SchemaClassTask, SchemaClassLegacyTask


def test_get_schema_name_registration(transactional_db):
    assert isinstance(get_schema_name, Task)
    assert not isinstance(get_schema_name, LegacyTask)
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_name'
    assert name == get_schema_name.name
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_name'
    assert isinstance(task, TenantTask)


@pytest.mark.xfail(reason="decorator tasks with custom base class not implemented")
def test_get_schema_from_class_task_registration(transactional_db):
    assert isinstance(get_schema_from_class_task, Task)
    assert not isinstance(get_schema_from_class_task, LegacyTask)
    
    name = 'tenant_schemas_celery.test_tasks.get_schema_from_class_task'
    assert name == get_schema_from_class_task.name
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'get_schema_from_class_task'
    assert isinstance(task, SchemaClassTask)
    assert isinstance(task, TenantTask)


def test_schema_class_legacy_task_registration(transactional_db):
    assert issubclass(SchemaClassLegacyTask, LegacyTask)
    
    name = 'tenant_schemas_celery.test_tasks.SchemaClassLegacyTask'
    assert name == SchemaClassLegacyTask.name
    assert name in app._tasks
    
    task = app._tasks[name]
    assert not inspect.isclass(task)
    assert task.__class__.__name__ == 'DynamicTenantTask'
    assert isinstance(task, TenantTask)
    assert isinstance(task, SchemaClassLegacyTask)
