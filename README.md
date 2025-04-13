tenant-schemas-celery
=====================

Celery application implementation that allows celery tasks to cooperate with the
[django-tenants](https://github.com/tomturner/django-tenants) package.

This project might not seem frequently updated, but it just has all the functionality needed. Issues and questions are answered quickly.

Installation
------------

```bash
   $ pip install tenant-schemas-celery
   $ pip install django-tenants
```

Usage
-----

   * Define a celery app using given `CeleryApp` class.

```python
   import os
   os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')

   from django.conf import settings

   from tenant_schemas_celery.app import CeleryApp as TenantAwareCeleryApp

   app = TenantAwareCeleryApp()
   app.config_from_object('django.conf:settings')
   app.autodiscover_tasks(lambda: settings.INSTALLED_APPS)
```

This assumes a fresh Celery 5.4.0 application. For previous versions, the key is to create a new `CeleryApp` instance that will be used to access task decorator from.

   * Replace your `@task` decorator with `@app.task`

```python
   from celery import shared_task
   from django.db import connection
   from myproject.celery import app
   from tenant_schemas_celery.task import TenantTask

   @app.task
   def my_task():
      print(connection.schema_name)

   @shared_task(base=TenantTask, bind=True)
   def my_shared_task():
      print("foo")
```

   * Run celery worker (`myproject.celery` is where you've defined the `app` variable)

```bash
    $ celery worker -A myproject.celery
```

   * Post registered task. The schema name will get automatically added to the task's arguments.

```python
   from myproject.tasks import my_task
   my_task.delay()
```

The `TenantTask` class transparently inserts current connection's schema into
the task's kwargs. The schema name is then popped from the task's kwargs in
`task_prerun` signal handler, and the connection's schema is changed
accordingly.

### Multiple databases support

Inside your celery tasks you might be working with multiple databases. You might want to change the schema for
all of the connections, or just a subset of them.

You can now use the `CELERY_TASK_TENANT_CACHE_SECONDS` django setting, or `TASK_TENANT_CACHE_SECONDS` celery setting, or
the `tenant_databases` attribute of the `TenantTask` to a list of database names (the key in the `settings.DATABASES` dictionary).

If not set, the settings defaults to `["default"]`.

### Tenant objects cache

Every time a celery task is executed, the tenant object of the `connection` object is being refetched.
For some use cases, this can introduce significant performance hit.

In such scenarios, you can pass `tenant_cache_seconds` argument to the `@app.task()` decorator. This will
cause the tenant objects to be cached for given period of time. `0` turns this off. You can also enable cache globally
by setting celery's `TASK_TENANT_CACHE_SECONDS` (app-specific, usually it's `CELERY_TASK_TENANT_CACHE_SECONDS`).

```python
@app.task(tenant_cache_seconds=30)
def some_task():
    ...
```

### Celery beat integration

In order to run celery beat tasks in a multi-tenant environment, you've got the following options:
- Use a dispatching task that will send a task for each tenant
- Use a custom scheduler
- Use django-celery-beat package

i.e: Let's say that you would like to run a `reset_remaining_jobs` tasks periodically, for every tenant that you have.

#### Dispatcher task pattern
You can schedule one dispatcher task that will iterate over all schemas and send that task within the schema's context:

```python
from django_tenants.utils import get_tenant_model, tenant_context
from django_tenant_schemas.utils import get_tenant_model, tenant_context

@app.task
def reset_remaining_jobs_in_all_schemas():
    for tenant in get_tenant_model().objects.exclude(schema_name='public'):
        with tenant_context(tenant):
            reset_remaining_jobs_in_schema.delay()

@app.task
def reset_remaining_jobs_in_schema():
    <do some logic>
```

The `reset_remaining_jobs_in_all_schemas` task (called the dispatch task) should be registered in your celery beat schedule. The `reset_remaining_jobs_in_schema` task should be called from the dispatch task.

That way you have full control over which schemas the task should be scheduled in.

**Note:** All tasks will be scheduled immediately across all tenants, possibly creating the thundering herd problem.


#### Custom scheduler
If you are using the standard `Scheduler` or `PersistentScheduler` classes provided by `celery`, you can transition to using this package's `TenantAwareScheduler` or `TenantAwarePersistentScheduler` classes. You should then specify the scheduler you want to use in the celery beat config or your invocation to `beat`. i.e:

```bash
celery -A proj beat --scheduler=tenant_schemas_celery.scheduler.TenantAwareScheduler
```

##### Caveats
- There's a chance that celery beat will try to run a task for a newly created tenant before its migrations are ready, which could potentially lead to deadlocks. This is specially true with big projects with a lot of migrations and very frequent tasks (i.e: every minute). In order to mitigate it, one could do the following:
    1. Subclass any of `TenantAwareScheduler` or `TenantAwarePersistentScheduler` and override the `get_queryset` method to match your definition of "ready" tenants. For example, imagine that you had a `ready` flag in your tenant model. You could do the following:

    ```python
    # tenants_app/scheduler.py
    class MyTenantAwareScheduler(TenantAwareScheduler):
        @classmethod
        def get_queryset(cls):
            return super().get_queryset().filter(ready=True)
    ```

    2. Use the new scheduler in your celery beat config or invocation:

    ```bash
    celery -A proj beat --scheduler=tenants_app.scheduler.MyTenantAwareScheduler
    ```

- `TenantAwareSchedulerMixin` uses a subclass of `SchedulerEntry` that allows the user to provide specific schemas to run a task on. This might prove useful if you have a task you only want to run in the `public` schema or to a subset of your tenants. In order to set that, you must configure `tenant_schemas` in the tasks definition as such:

```python
app.conf.beat_schedule = {
    "my-task": {
        "task": "myapp.tasks.my_task",
        "schedule": schedules.crontab(minute="*/5"),
        "tenant_schemas": ["public"]
    }
}
```

#### django-celery-beat integration

You can use the `tenant_schemas_celery.db_scheduler.TenantAwareDatabaseScheduler` scheduler to integrate the `django-celery-beat` package with multiple tenants.
The scheduler will pull all periodic tasks defined in every tenant schema and public schema, and add them to the scheduler's registry.

Because tasks from schemas are merged into a single registry, tasks' names must be unique. For example, you can add the `@<schema_name>` suffix to each of the entries:

```python
# In public schema
PeriodicTask.objects.create(name="my_task@public", task="my_app.tasks.my_task")
# In schema "tenant"
PeriodicTask.objects.create(name="my_task@tenant", task="my_app.tasks.my_task")
```

The package will raise `ValueError` if entries with conflicting names are detected.

**Note:** Since every periodic task defined in the database can have different schedule (incl. offset), this allows you to avoid the thundering herd problem.

Compatibility changes
=====================

The `>=4.0` series drop support for Python 3.8 and Celery<5.

The `>=3.0` series drop support for Python 3.7.

The `>=2.1` series drop support for `tenant-schemas`. It hasn't been maintainted for
a long time.

The `2.x` series support Python>=3.7.

The `1.x` series support Python>=3.6. Python 3.6 reached EOL 2021-12.

The `0.x` series are the last one to support Python<3.6.
