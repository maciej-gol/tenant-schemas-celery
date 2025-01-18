import os

try:
    from tenant_schemas_celery.app import CeleryApp
except ImportError:
    app = None
else:
    app = CeleryApp('testapp')

    class CeleryConfig:
        BROKER_URL = os.environ.get("BROKER_URL", "amqp://")
        CELERY_RESULT_BACKEND = 'rpc://'
        CELERY_RESULT_PERSISTENT = False
        CELERY_ALWAYS_EAGER = False

        CELERYBEAT_SCHEDULE = {
            'test-periodic-task': {
                'task': 'tenant_schemas_celery.test_tasks.print_all_schemas',
                'schedule': 4.0,
            },
            'test-periodic-task-smth': {
                'task': 'tenant_schemas_celery.test_tasks.print_schema',
                'schedule': 4.0,
                'tenant_schemas': ['tenant-1'],
            },
        }

    app.config_from_object(CeleryConfig)
    app.autodiscover_tasks(['tenant_schemas_celery'], 'test_tasks')
