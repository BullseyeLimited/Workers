import os, json, time, traceback, requests
from supabase import create_client

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


def process(job):
    data = json.loads(job["message"])
    prompt = TPL.format(**data)
    summary = call_llm(prompt)

    SB.table("plan_history").insert(
        {
            "thread_id": data["thread_id"],
            "horizon": data["horizon"],
            "plan_status": data["plan_status"],
            "previous_plan": data["previous_plan"],
            "reason_for_change": data["reason_for_change"],
            "summary_json": json.dumps(summary, ensure_ascii=False),
        }
    ).execute()


def main():
    while True:
        job = SB.rpc("pgmq_receive", {"q_name": QUEUE, "vt": 30}).data
        if not job:
            time.sleep(1)
            continue
        try:
            process(job)
            SB.rpc("pgmq_delete", {"q_name": QUEUE, "msg_id": job["msg_id"]})
        except Exception:
            traceback.print_exc()


if __name__ == "__main__":
    main()
