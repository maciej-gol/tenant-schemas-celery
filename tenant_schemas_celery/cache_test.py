from datetime import datetime, timedelta

from freezegun import freeze_time

from tenant_schemas_celery.cache import SimpleCache


def test_cache_get_should_return_default_value_if_key_doesnt_exist():
    cache = SimpleCache()
    expected_value = object()

    actual_value = cache.get("something-non-existant", expected_value)

    assert actual_value is expected_value


def test_cache_set_should_add_value_to_cache():
    cache = SimpleCache()
    expected_value = "stored-value"

    cache.set("new_key", expected_value, 1)
    actual_value = cache.get("new_key", default=None)

    assert actual_value is expected_value


def test_cache_get_should_return_default_value_if_key_expired():
    expire_seconds = 1
    cache = SimpleCache()
    expected_value = "default-value"
    now = datetime.utcnow()

    with freeze_time(now):
        cache.set("new_key", "stored-value", expire_seconds)

    with freeze_time(now + timedelta(seconds=2 * expire_seconds)):
        actual_value = cache.get("new_key", default=expected_value)

    assert actual_value is expected_value


def test_cache_should_allow_reusing_storage():
    storage = {}
    cache1 = SimpleCache(storage=storage)
    expected_value = "x"
    cache1.set("some-key", expected_value, expire_seconds=1)
    cache2 = SimpleCache(storage=storage)

    actual_value = cache2.get("some-key", "y")

    assert actual_value is expected_value
