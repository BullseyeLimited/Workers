from __future__ import annotations

from typing import Any, Dict


def record_ai_response(
    client,
    *,
    worker: str,
    thread_id: int | None = None,
    message_id: int | None = None,
    run_id: str | None = None,
    model: str | None = None,
    request_payload: Dict[str, Any] | None = None,
    response_payload: Dict[str, Any] | None = None,
    status: str | None = None,
    error: str | None = None,
) -> None:
    if response_payload is None:
        return

    row = {
        "worker": worker,
        "thread_id": thread_id,
        "message_id": message_id,
        "run_id": run_id,
        "model": model,
        "request_json": request_payload,
        "response_json": response_payload,
        "status": status,
        "error": error,
    }
    row = {key: value for key, value in row.items() if value is not None}

    try:
        client.table("ai_raw_responses").insert(row).execute()
    except Exception as exc:  # noqa: BLE001
        print(f"[ai_raw_responses] insert failed: {exc}")
