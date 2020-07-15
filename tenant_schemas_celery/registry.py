import inspect

from celery.app.registry import TaskRegistry

from .task import TenantTask


class TenantTaskRegistry(TaskRegistry):
    def register(self, task):
        if inspect.isclass(task) and not issubclass(task, TenantTask):
            # HANDLE AUTOMATIC REGISTRATION OF LEGACY CLASS BASED TASKS
            # https://github.com/celery/celery/blob/v4.4.6/celery/task/base.py#L117
            class DynamicTenantTask(task, TenantTask):
                name = task.name
            task = DynamicTenantTask
        super().register(task)
