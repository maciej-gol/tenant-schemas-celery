try:
    from .app import CeleryApp
except ImportError:
    app = None
else:
    app = CeleryApp('testapp')

    class CeleryConfig:
        BROKER_URL = 'amqp://'
        CELERY_RESULT_BACKEND = 'rpc://'
        CELERY_RESULT_PERSISTENT = False
        CELERY_ALWAYS_EAGER = False
        CELERY_TASK_TENANT_CACHE_SECONDS = os.environ.get("TASK_TENANT_CACHE_SECONDS", 10)

    app.config_from_object(CeleryConfig)
    app.autodiscover_tasks(['tenant_schemas_celery'], 'test_tasks')
