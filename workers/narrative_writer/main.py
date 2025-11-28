"""
Unified narrative summarization worker.

Rolls up turns into episode summaries (every 20 turns by default) and then
cascades rollups: 3 episodes -> 1 chapter, 3 chapters -> 1 season,
3 seasons -> 1 year, 3 years -> 1 lifetime.

Each tier uses its own prompt template under /prompts and writes
{narrative_summary, abstract_summary} into the public.summaries table.
"""

from __future__ import annotations

import json
import os
import time
import traceback
from pathlib import Path
from typing import Any, Dict, List, Tuple

import requests
from supabase import ClientOptions, create_client

from workers.lib.json_utils import safe_parse_model_json
from workers.lib.simple_queue import ack, receive

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise RuntimeError("Missing Supabase configuration for narrative_writer")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)

RUNPOD_URL = os.getenv("RUNPOD_URL", "").rstrip("/")
RUNPOD_MODEL = os.getenv("RUNPOD_MODEL_NAME", "qwq-32b-ablit")
RUNPOD_API_KEY = os.getenv("RUNPOD_API_KEY", "")

QUEUE = os.getenv("NARRATIVE_QUEUE", "narrative.write")

EPISODE_WINDOW = int(os.getenv("EPISODE_WINDOW", "20"))
ROLLOVER = int(os.getenv("ROLLOVER_FACTOR", "3"))

PROMPTS_DIR = Path(__file__).resolve().parents[2] / "prompts"
PROMPT_FILES = {
    "episode": PROMPTS_DIR / "narrative_episode.txt",
    "chapter": PROMPTS_DIR / "narrative_chapter.txt",
    "season": PROMPTS_DIR / "narrative_season.txt",
    "year": PROMPTS_DIR / "narrative_year.txt",
    "lifetime": PROMPTS_DIR / "narrative_lifetime.txt",
}
OUTPUT_KEYS = {
    "episode": "episode_narrative",
    "chapter": "chapter_narrative",
    "season": "season_narrative",
    "year": "year_narrative",
    "lifetime": "lifetime_narrative",
}
PROMPT_SPLIT_MARKER = "=== INPUT"


def runpod_call(system_prompt: str, user_prompt: str) -> str:
    if not RUNPOD_URL:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{RUNPOD_URL}/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {RUNPOD_API_KEY}",
    }
    body = {
        "model": RUNPOD_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 4096,
        "temperature": 0.4,
    }
    resp = requests.post(url, headers=headers, json=body, timeout=120)
    resp.raise_for_status()
    data = resp.json()
    choice = data["choices"][0]
    raw_text = choice.get("text") or (choice.get("message") or {}).get("content") or ""
    return raw_text


def _load_prompt(tier: str) -> str:
    path = PROMPT_FILES[tier]
    if not path.exists():
        raise FileNotFoundError(f"Prompt file missing for {tier}: {path}")
    return path.read_text(encoding="utf-8")


def _split_prompt(prompt: str) -> tuple[str, str]:
    idx = prompt.find(PROMPT_SPLIT_MARKER)
    if idx == -1:
        return prompt, "Return only the JSON payload."
    return prompt[:idx].strip(), prompt[idx:].strip()


def _format_messages(rows: List[Dict[str, Any]]) -> str:
    lines: List[str] = []
    for row in rows:
        sender = (row.get("sender") or "-").strip()[:1].upper() or "-"
        turn = row.get("turn_index") or "-"
        text = row.get("message_text") or ""
        lines.append(f"[{turn} {sender}] {text}")
    return "\n".join(lines)


def _format_child_summaries(rows: List[Dict[str, Any]]) -> str:
    blocks: List[str] = []
    for row in rows:
        label = f"{row.get('tier','-').title()} {row.get('tier_index','-')}"
        nar = (row.get("narrative_summary") or "").strip()
        abr = (row.get("abstract_summary") or "").strip()
        parts = [f"{label} - Narrative:\n{nar}", f"{label} - Abstract:\n{abr}"]
        blocks.append("\n".join(parts))
    return "\n\n".join(blocks)


def _insert_summary(
    tier: str,
    tier_index: int,
    thread_id: int,
    narrative: str,
    abstract: str,
    start_turn: int | None,
    end_turn: int | None,
) -> None:
    payload = {
        "thread_id": thread_id,
        "tier": tier,
        "tier_index": tier_index,
        "start_turn": start_turn,
        "end_turn": end_turn,
        "narrative_summary": narrative,
        "abstract_summary": abstract,
    }
    SB.table("summaries").insert(payload).execute()


def _fetch_summaries(thread_id: int, tier: str) -> List[Dict[str, Any]]:
    res = (
        SB.table("summaries")
        .select(
            "id,thread_id,tier,tier_index,start_turn,end_turn,narrative_summary,abstract_summary"
        )
        .eq("thread_id", thread_id)
        .eq("tier", tier)
        .order("tier_index")
        .execute()
    )
    return res.data or []


def _fetch_messages(thread_id: int, start_turn: int, end_turn: int) -> List[Dict[str, Any]]:
    res = (
        SB.table("messages")
        .select("turn_index,sender,message_text")
        .eq("thread_id", thread_id)
        .gte("turn_index", start_turn)
        .lte("turn_index", end_turn)
        .order("turn_index")
        .execute()
    )
    return res.data or []


def _build_episode_prompt(messages: List[Dict[str, Any]]) -> str:
    tpl = _load_prompt("episode")
    return tpl.replace("{RAW_TURNS}", _format_messages(messages))


def _build_rollup_prompt(tier: str, child_rows: List[Dict[str, Any]]) -> str:
    tpl = _load_prompt(tier)
    return tpl.replace("{CHILD_SUMMARIES}", _format_child_summaries(child_rows))


def _parse_model_output(raw_text: str) -> Tuple[Dict[str, Any] | None, str | None]:
    data, err = safe_parse_model_json(raw_text)
    if err is not None:
        return None, err
    return data, None


def extract_narrative(tier: str, raw_text: str) -> str:
    """
    Parse model output and pull the tier-specific narrative key.
    Raises ValueError on parse failure or missing key.
    """
    data, err = safe_parse_model_json(raw_text)
    if err or not data:
        raise ValueError(f"parse error: {err}")
    key = OUTPUT_KEYS[tier]
    val = data.get(key)
    if not val:
        raise ValueError(f"missing {key} in model output")
    return val


def _episode_due(turn_count: int, episodes_done: int) -> List[Tuple[int, int, int]]:
    target = turn_count // EPISODE_WINDOW
    due = []
    for idx in range(episodes_done + 1, target + 1):
        start = (idx - 1) * EPISODE_WINDOW + 1
        end = idx * EPISODE_WINDOW
        due.append((idx, start, end))
    return due


def _rollup_due(child_count: int, current_count: int) -> List[int]:
    target = child_count // ROLLOVER
    return list(range(current_count + 1, target + 1))


def _chunk_for_index(child_rows: List[Dict[str, Any]], tier_index: int, chunk_size: int) -> List[Dict[str, Any]]:
    """
    Return the specific non-overlapping chunk for this tier_index.
    tier_index is 1-based; chunk_size is usually ROLLOVER (3).
    """
    start_idx = (tier_index - 1) * chunk_size
    end_idx = start_idx + chunk_size
    return child_rows[start_idx:end_idx]


def _process_episode(thread_id: int, turn_count: int) -> None:
    existing = _fetch_summaries(thread_id, "episode")
    due = _episode_due(turn_count, len(existing))
    for tier_index, start, end in due:
        msgs = _fetch_messages(thread_id, start, end)
        prompt = _build_episode_prompt(msgs)
        sys_prompt, user_prompt = _split_prompt(prompt)
        try:
            raw = runpod_call(sys_prompt, user_prompt)
            nar = extract_narrative("episode", raw)
        except Exception as exc:  # noqa: BLE001
            print(f"[narrative_writer] RunPod/parse error (episode {tier_index}): {exc}", flush=True)
            continue
        _insert_summary("episode", tier_index, thread_id, nar, "", start, end)
        print(f"[narrative_writer] Stored episode {tier_index} ({start}-{end}) for thread {thread_id}", flush=True)


def _process_rollup(thread_id: int, tier: str, child_tier: str) -> None:
    child_rows = _fetch_summaries(thread_id, child_tier)
    this_rows = _fetch_summaries(thread_id, tier)
    due = _rollup_due(len(child_rows), len(this_rows))
    for tier_index in due:
        block = _chunk_for_index(child_rows, tier_index, ROLLOVER)
        if len(block) < ROLLOVER:
            # Not enough child summaries to build this rollup yet.
            continue
        prompt = _build_rollup_prompt(tier, block)
        sys_prompt, user_prompt = _split_prompt(prompt)
        try:
            raw = runpod_call(sys_prompt, user_prompt)
            nar = extract_narrative(tier, raw)
        except Exception as exc:  # noqa: BLE001
            print(f"[narrative_writer] RunPod/parse error ({tier} {tier_index}): {exc}", flush=True)
            continue

        start_turn = block[0].get("start_turn")
        end_turn = block[-1].get("end_turn")
        _insert_summary(tier, tier_index, thread_id, nar, "", start_turn, end_turn)
        print(f"[narrative_writer] Stored {tier} {tier_index} for thread {thread_id}", flush=True)


def process_thread(thread_id: int) -> None:
    thr = (
        SB.table("threads")
        .select("turn_count")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
    )
    if not thr:
        print(f"[narrative_writer] thread {thread_id} not found", flush=True)
        return

    turn_count = thr.get("turn_count") or 0
    _process_episode(thread_id, turn_count)
    _process_rollup(thread_id, "chapter", "episode")
    _process_rollup(thread_id, "season", "chapter")
    _process_rollup(thread_id, "year", "season")
    _process_rollup(thread_id, "lifetime", "year")


def main() -> None:
    print("[narrative_writer] started - waiting for jobs", flush=True)
    while True:
        job = receive(QUEUE, 60)
        if not job:
            time.sleep(1)
            continue
        row_id = job["row_id"]
        payload = job.get("payload") or {}
        thread_id = payload.get("thread_id")
        if not thread_id:
            print("[narrative_writer] missing thread_id in payload; skipping", flush=True)
            ack(row_id)
            continue
        try:
            process_thread(int(thread_id))
        except Exception as exc:  # noqa: BLE001
            print("[narrative_writer] error:", exc, flush=True)
            traceback.print_exc()
        finally:
            ack(row_id)


if __name__ == "__main__":
    main()
