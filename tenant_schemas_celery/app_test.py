from tenant_schemas_celery.app import CeleryApp
from tenant_schemas_celery.task import TenantTask


class DummyTask(TenantTask):
    ...


def test_celery_app_should_allow_overriding_task_cls_as_object() -> None:
    class App(CeleryApp):
        task_cls = DummyTask

    app = App(set_as_current=False)

    @app.task()
    def some_task() -> None:
        ...

    assert isinstance(some_task, DummyTask)


def test_celery_app_should_allow_overriding_task_cls_as_string() -> None:
    class App(CeleryApp):
        task_cls = f"{DummyTask.__module__}:{DummyTask.__name__}"

    app = App(set_as_current=False)

    @app.task()
    def some_task() -> None:
        ...

    assert isinstance(some_task, DummyTask)
