import json, os, time
import traceback

import requests
from supabase import create_client, ClientOptions

from workers.lib.prompt_builder import build_prompt, live_turn_window
from workers.lib.simple_queue import receive, ack, send

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

SB = create_client(
    SUPABASE_URL,
    SUPABASE_KEY,
    options=ClientOptions(
        headers={
            "Authorization": f"Bearer {SUPABASE_KEY}",
        }
    ),
)
QUEUE = "napoleon.reply"
HORIZONS = ["episode", "chapter", "season", "year", "lifetime"]


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


def process_job(payload):
    fan_msg_id = payload["message_id"]
    thread_id = payload["thread_id"]

    turns = live_turn_window(thread_id)
    prompt = build_prompt("napoleon", thread_id, turns)

    rsp = requests.post(
        os.getenv("RUNPOD_URL"),
        headers={"Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY')}"},
        json={"prompt": prompt},
    ).json()
    out = json.loads(rsp["output"])

    rethink = out["RETHINK_HORIZONS"]
    if rethink != "no":
        multi_plan = out["MULTI_HORIZON_PLAN"]
        for hz in HORIZONS:
            new_plan = multi_plan[hz]["PLAN"]
            status = multi_plan[hz]["STATE"]
            reason = (
                multi_plan[hz]["NOTES"][0] if multi_plan[hz]["NOTES"] else ""
            )
            col = f"{hz}_plan"

            old_plan = (
                SB.table("threads")
                .select(col)
                .eq("id", thread_id)
                .single()
                .execute()
                .data.get(col)
                or ""
            )

            if new_plan != old_plan:
                send(
                    "plans.archive",
                    {
                        "fan_message_id": fan_msg_id,
                        "thread_id": thread_id,
                        "horizon": hz,
                        "previous_plan": old_plan,
                        "plan_status": status,
                        "reason_for_change": reason,
                    },
                )

                SB.table("threads").update({col: new_plan}).eq(
                    "id", thread_id
                ).execute()

    creator_msg_id = insert_creator_reply(thread_id, out["FINAL_MESSAGE"])

    upsert_napoleon_details(fan_msg_id, thread_id, out)
    SB.table("messages").update({"napoleon_output": json.dumps(out)}).eq(
        "id", creator_msg_id
    ).execute()


if __name__ == "__main__":
    while True:
        job = receive(QUEUE, 30)
        if not job:
            time.sleep(1); continue
        try:
            process_job(job["payload"])
            ack(job["row_id"])
        except Exception:
            traceback.print_exc()
