import pytest

from closeyourit import reset_scope


@pytest.fixture(autouse=True)
def isolate_scope_between_tests() -> None:
    reset_scope()
