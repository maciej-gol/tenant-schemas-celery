import os

try:
    from .app import CeleryApp
except ImportError:
    app = None
else:
    app = CeleryApp('testapp')

    class CeleryConfig:
        BROKER_URL = os.environ.get("BROKER_URL", "amqp://")
        CELERY_RESULT_BACKEND = 'rpc://'
        CELERY_RESULT_PERSISTENT = False
        CELERY_ALWAYS_EAGER = False

    app.config_from_object(CeleryConfig)
    app.autodiscover_tasks(['tenant_schemas_celery'], 'test_tasks')
