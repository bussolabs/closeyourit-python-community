# CloseYourIt Python SDK

Official Python SDK for [CloseYourIt](https://closeyour.it), the open source tracker for errors,
logs, uptime and servers. It sends errors, logs and metrics from server-side Python applications.

Integrations: WSGI, ASGI, Django, Flask, FastAPI, Starlette, Celery, `requests`, `httpx` and
SQLAlchemy.

## Install

Python 3.11 or later. The core has no runtime dependencies; integrations install per stack:

```bash
python -m pip install closeyourit
python -m pip install "closeyourit[django]"   # or flask, fastapi, starlette, celery, requests, httpx, sqlalchemy
```

## Configuration

| Variable | Meaning |
|---|---|
| `CLOSEYOURIT_ENDPOINT_URL` | Ingest URL of your CloseYourIt |
| `CLOSEYOURIT_TOKEN` | Server-side bearer token (`cyi_…`); never expose it to public clients |
| `CLOSEYOURIT_PROJECT_ID` | CloseYourIt project id |
| `CLOSEYOURIT_ENVIRONMENT` | Application environment |
| `CLOSEYOURIT_RELEASE` | Version of the monitored application |
| `CLOSEYOURIT_USAGE_ENABLED` | Turns on usage telemetry, off by default (`1`, `true`, `yes`, `on`) |

An incomplete or invalid configuration disables the SDK without raising. In `production` the
endpoint must use HTTPS.

```python
from closeyourit import Configuration

configuration = Configuration()
if not configuration.enabled:
    print(configuration.disabled_reason)
```

Full guide, one section per integration: [closeyour.it/docs/sdk-python](https://closeyour.it/docs/sdk-python).

## Development

```bash
mise install
mise exec -- python -m venv .venv
.venv/bin/python -m pip install -e ".[dev]"
.venv/bin/ruff check . && .venv/bin/ruff format --check . && .venv/bin/mypy
.venv/bin/python -m pytest
```

## Contributing

Pull requests are welcome: read [CONTRIBUTING.md](CONTRIBUTING.md) and [CLA.md](CLA.md). This
repository is a published copy: accepted changes are applied to the upstream source and come back
here with the next release. Report vulnerabilities as described in [SECURITY.md](SECURITY.md).

## License

MIT, see `LICENSE`.
