import json, os, time
from datetime import datetime, timezone

import requests
from supabase import create_client

from lib.prompt_builder import build_prompt, live_turn_window

SB = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)
QUEUE = "napoleon.reply"


def upsert_napoleon_details(msg_id: int, thread_id: int, out: dict):
    row = {
        "message_id": msg_id,
        "thread_id": thread_id,
        "sender": "fan",
        "tactical_plan_3turn": json.dumps(out["TACTICAL_PLAN_3TURNS"]),
        "plan_episode": json.dumps(out["MULTI_HORIZON_PLAN"]["EPISODE"]),
        "plan_chapter": json.dumps(out["MULTI_HORIZON_PLAN"]["CHAPTER"]),
        "plan_season": json.dumps(out["MULTI_HORIZON_PLAN"]["SEASON"]),
        "plan_year": json.dumps(out["MULTI_HORIZON_PLAN"]["YEAR"]),
        "plan_lifetime": json.dumps(out["MULTI_HORIZON_PLAN"]["LIFETIME"]),
        "rethink_horizons": json.dumps(out["RETHINK_HORIZONS"]),
        "napoleon_final_message": out["FINAL_MESSAGE"],
        "extract_status": "ok",
        "extracted_at": datetime.now(timezone.utc).isoformat(),
    }
    SB.table("message_ai_details").upsert(row, on_conflict="message_id").execute()


def insert_creator_reply(thread_id: int, final_text: str) -> int:
    # fetch next turn index
    thr = (
        SB.table("threads")
        .select("turn_count")
        .eq("id", thread_id)
        .single()
        .execute()
        .data
    )
    next_turn = (thr["turn_count"] or 0) + 1
    row = {
        "thread_id": thread_id,
        "turn_index": next_turn,
        "sender": "creator",
        "message_text": final_text,
        "ext_message_id": f"auto-{int(time.time()*1000)}",
        "source_channel": "scheduler",
    }
    msg = SB.table("messages").insert(row).execute().data[0]
    return msg["id"]


# -------- main poll loop --------
while True:
    job = (
        SB.postgrest.rpc("pgmq_poll", {"q_name": QUEUE, "vt": 30})
        .execute()
        .data
    )
    if not job:
        time.sleep(2)
        continue

    payload = job["message"]
    fan_msg_id = payload["message_id"]
    thread_id = payload["thread_id"]

    # build prompt
    turns = live_turn_window(thread_id)
    prompt = build_prompt("napoleon", thread_id, turns)

    # call RunPod / OpenAI
    rsp = requests.post(
        os.getenv("RUNPOD_URL"),
        headers={"Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY')}"},
        json={"prompt": prompt},
    ).json()
    out = json.loads(rsp["output"])

    # 1) write creator reply row
    creator_msg_id = insert_creator_reply(thread_id, out["FINAL_MESSAGE"])

    # 2) upsert ai_details for the triggering fan message
    upsert_napoleon_details(fan_msg_id, thread_id, out)
    SB.table("messages").update({"napoleon_output": json.dumps(out)}).eq(
        "id", fan_msg_id
    ).execute()

    # ack job
    SB.postgrest.rpc(
        "pgmq_complete", {"q_name": QUEUE, "msg_id": job["msg_id"]}
    ).execute()
