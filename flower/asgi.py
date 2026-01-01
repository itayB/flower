from __future__ import annotations
from importlib.resources import files as resource_files
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import APIRouter, FastAPI
from fastapi.staticfiles import StaticFiles
from tornado.ioloop import IOLoop
from tornado.options import options

from .events import Events
from .inspector import Inspector
from .routers import tasks as tasks_router


def _normalized_url_prefix() -> str:
    raw = (getattr(options, "url_prefix", None) or "").strip("/")
    return f"/{raw}" if raw else ""


@dataclass
class FlowerRuntime:
    capp: Any
    io_loop: Optional[IOLoop]
    io_loop_thread: threading.Thread
    events: Optional[Events]
    inspector: Optional[Inspector]


def create_app(*, capp: Any) -> FastAPI:
    """Create a FastAPI app that can run alongside the existing Tornado server.

    This is intentionally minimal: it boots Flower's background runtime (IOLoop,
    Events, Inspector) but does not attempt to reimplement the Tornado handlers.
    """

    app = FastAPI(title="Flower (FastAPI)")

    ready = threading.Event()
    init_error: dict[str, BaseException] = {}

    def _run_loop() -> None:
        try:
            io_loop = IOLoop()
            io_loop.make_current()
            io_loop.set_default_executor(ThreadPoolExecutor())

            events = Events(
                capp,
                io_loop,
                db=options.db,
                persistent=options.persistent,
                state_save_interval=options.state_save_interval,
                enable_events=options.enable_events,
                max_workers_in_memory=options.max_workers,
                max_tasks_in_memory=options.max_tasks,
            )
            inspector = Inspector(io_loop, capp, options.inspect_timeout / 1000.0)

            runtime: FlowerRuntime = app.state.flower_runtime
            runtime.io_loop = io_loop
            runtime.events = events
            runtime.inspector = inspector

            events.start()
            ready.set()
            io_loop.start()
        except BaseException as exc:  # pragma: no cover
            init_error["exc"] = exc
            ready.set()

    io_loop_thread = threading.Thread(target=_run_loop, name="flower-ioloop")
    io_loop_thread.daemon = True

    app.state.flower_runtime = FlowerRuntime(
        capp=capp,
        io_loop=None,
        io_loop_thread=io_loop_thread,
        events=None,
        inspector=None,
    )

    url_prefix = _normalized_url_prefix()
    router = APIRouter(prefix=url_prefix)

    @router.get("/api/healthcheck")
    async def healthcheck() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(router)
    app.include_router(tasks_router.router, prefix=url_prefix)

    @app.on_event("startup")
    def _startup() -> None:
        runtime: FlowerRuntime = app.state.flower_runtime
        runtime.io_loop_thread.start()
        ready.wait(timeout=5)
        if "exc" in init_error:  # pragma: no cover
            raise init_error["exc"]
        if runtime.events is None or runtime.io_loop is None:  # pragma: no cover
            raise RuntimeError("Flower runtime did not initialize")

    @app.on_event("shutdown")
    def _shutdown() -> None:
        runtime: FlowerRuntime = app.state.flower_runtime
        if runtime.events is not None:
            runtime.events.stop()
        if runtime.io_loop is not None:
            runtime.io_loop.add_callback(runtime.io_loop.stop)
        runtime.io_loop_thread.join(timeout=2)

    frontend_path = resource_files("frontend").joinpath("web-dist")
    app.mount("/", StaticFiles(directory=f"{frontend_path}/"))

    return app
