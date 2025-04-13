import pytest

from celery import current_app

from tenant_schemas_celery.test_utils import ClientFactory


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


@pytest.fixture
def client_factory() -> ClientFactory:
    with ClientFactory() as factory:
        yield factory
