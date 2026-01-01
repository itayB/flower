from asyncio import Event
from collections import OrderedDict
from typing import Any

from fastapi import APIRouter, Request, status

from ..utils import tasks


readiness_event = Event()

router = APIRouter(
    prefix="/api/v2",
    tags=["tasks"],
)


@router.get(
    "/tasks",
    summary="get tasks",
    response_description="Return HTTP Status Code 200 (OK)",
    status_code=status.HTTP_200_OK,
)
async def get_tasks(
    request: Request,
    limit: int | None = None,
    offset: int = 0,
    workername: str | None = None,
    taskname: str | None = None,
    state: str | None = None,
    received_start: str | None = None,
    received_end: str | None = None,
    sort_by: str | None = None,
    search: str | None = None,
):
    runtime = request.app.state.flower_runtime
    events = getattr(runtime, "events", None)

    if events is None:
        return {"tasks": OrderedDict(), "total": 0}

    # Normalize and filter query parameters similar to the Tornado ListTasks handler
    offset = max(offset, 0)

    worker = workername if workername and workername != "All" else None
    task_type = taskname if taskname and taskname != "All" else None
    state_filter = state if state and state != "All" else None

    results: list[tuple[str, dict[str, Any]]] = []

    for task_id, task in tasks.iter_tasks(
        events,
        limit=limit,
        offset=offset,
        sort_by=sort_by,
        type=task_type,
        worker=worker,
        state=state_filter,
        received_start=received_start,
        received_end=received_end,
        search=search,
    ):
        task_dict = tasks.as_dict(task)
        worker_info = task_dict.pop("worker", None)
        if worker_info is not None:
            task_dict["worker"] = worker_info.hostname
        results.append((task_id, task_dict))

    return {
        "tasks": OrderedDict(results),
        "total": len(events.state.tasks),
    }
