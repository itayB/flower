from __future__ import annotations

import sys
from typing import List

import celery

from tornado.options import options

from .command import apply_env_options, apply_options


def _ensure_fastapi_deps() -> None:
    try:
        import fastapi  # noqa: F401
        import uvicorn  # noqa: F401
    except Exception as exc:  # pragma: no cover
        raise SystemExit(
            "FastAPI server dependencies are not installed. "
            "Install with: `pip install -e .[fastapi]` (or add fastapi/uvicorn). "
            f"Original error: {exc}"
        )


def main(argv: List[str] | None = None) -> None:
    """Run Flower using FastAPI/uvicorn.

    This is a parallel entrypoint intended for gradual migration.
    It reuses Flower's existing tornado.options configuration.

    Usage:
      python -m flower.fastapi [flower options]

    Example:
      python -m flower.fastapi --port=5556 --persistent=True --db=flower_db
    """

    _ensure_fastapi_deps()
    import uvicorn

    if argv is None:
        argv = sys.argv[1:]

    apply_env_options()
    apply_options(sys.argv[0], argv)

    capp = celery.Celery()
    capp.loader.import_default_modules()

    from .asgi import create_app

    app = create_app(capp=capp)

    uvicorn.run(
        app,
        host=options.address or "0.0.0.0",
        port=options.port,
        log_level="info",
    )


if __name__ == "__main__":
    main()
