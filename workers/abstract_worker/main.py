"""
Unified abstract worker.

Handles all abstract summarization queues plus card patching in one process:
- episode.abstract
- chapter.abstract
- season.abstract
- year.abstract
- lifetime.abstract
- card.patch
"""

from __future__ import annotations

import os
import time
import traceback
from typing import Callable, Dict, List, Tuple

from workers.lib.simple_queue import ack, receive

from workers.episode_abstract_writer.main import process_job as process_episode
from workers.chapter_abstract_writer.main import process_job as process_chapter
from workers.season_abstract_writer.main import process_job as process_season
from workers.year_abstract_writer.main import process_job as process_year
from workers.lifetime_abstract_writer.main import process_job as process_lifetime
from workers.card_patch_applier.main import process_job as process_card_patch


QUEUE_HANDLERS: List[Tuple[str, Callable[[Dict], bool]]] = [
    ("card.patch", process_card_patch),
    ("episode.abstract", process_episode),
    ("chapter.abstract", process_chapter),
    ("season.abstract", process_season),
    ("year.abstract", process_year),
    ("lifetime.abstract", process_lifetime),
]

VISIBILITY_TIMEOUT = int(os.getenv("ABSTRACT_QUEUE_VT", "30"))
IDLE_SLEEP = float(os.getenv("ABSTRACT_IDLE_SLEEP", "1"))


def _receive_any(queues: List[Tuple[str, Callable[[Dict], bool]]]):
    for queue_name, handler in queues:
        job = receive(queue_name, VISIBILITY_TIMEOUT)
        if job:
            return job, handler
    return None, None


def main() -> None:
    print("[abstract_worker] started - waiting for abstract/card.patch jobs", flush=True)
    while True:
        job, handler = _receive_any(QUEUE_HANDLERS)
        if not job:
            time.sleep(IDLE_SLEEP)
            continue
        row_id = job["row_id"]
        payload = job.get("payload") or {}
        try:
            ok = handler(payload)
            if ok:
                ack(row_id)
        except Exception as exc:  # noqa: BLE001
            print("[abstract_worker] error:", exc, flush=True)
            traceback.print_exc()
            time.sleep(2)


if __name__ == "__main__":
    main()
