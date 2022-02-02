import pytest

from celery import current_app


@pytest.fixture
def celery_conf():
    """
    Fixture allowing to override celery configuration during test,
    then the fixture will rollback the changes after completion
    """

    # Makes a shallow copy of the configuration
    original = current_app.conf
    new_conf = current_app.config_from_object(dict(original))

    yield new_conf

    current_app.config_from_object(original)
