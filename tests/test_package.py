import re

import closeyourit


def test_package_exposes_a_semantic_version() -> None:
    assert re.fullmatch(r"\d+\.\d+\.\d+", closeyourit.__version__)
