from __future__ import annotations

import subprocess
import sys
import textwrap


def test_base_package_import_does_not_require_optional_frameworks() -> None:
    code = textwrap.dedent(
        """
        import importlib.abc
        import sys

        class BlockOptional(importlib.abc.MetaPathFinder):
            def find_spec(self, fullname, path, target=None):
                if fullname.split('.')[0] in {'requests', 'httpx', 'sqlalchemy'}:
                    raise ImportError(f'blocked optional dependency: {fullname}')
                return None

        sys.meta_path.insert(0, BlockOptional())
        import closeyourit
        assert callable(closeyourit.instrument_requests)
        assert callable(closeyourit.instrument_httpx)
        assert callable(closeyourit.instrument_sqlalchemy)
        """
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
