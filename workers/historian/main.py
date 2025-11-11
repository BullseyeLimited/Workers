import os, json, time, traceback, requests
from supabase import create_client
from workers.lib.simple_queue import receive, ack

SB = create_client(
    os.getenv("SUPABASE_URL"),
    os.getenv("SUPABASE_SERVICE_ROLE_KEY"),
)
QUEUE = "plans.archive"
TPL = open("/app/prompts/historian.txt").read()


def call_llm(prompt: str) -> dict:
    r = requests.post(
        os.getenv("RUNPOD_URL"),
        headers={
            "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY')}",
            "Content-Type": "application/json",
        },
        json={"model": "gpt-4o-mini", "prompt": prompt, "max_tokens": 350},
    )
    r.raise_for_status()
    return json.loads(r.json()["output"])


def process_job(payload):
    prompt = TPL.format(**payload)
    summary = call_llm(prompt)

    SB.table("plan_history").insert(
        {
            "thread_id": payload["thread_id"],
            "horizon": payload["horizon"],
            "plan_status": payload["plan_status"],
            "previous_plan": payload["previous_plan"],
            "reason_for_change": payload["reason_for_change"],
            "summary_json": json.dumps(summary, ensure_ascii=False),
        }
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
