import pytest

@pytest.fixture(autouse=True)
def cleanup_db_after_test():
    yield
