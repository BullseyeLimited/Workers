import os, json, time, traceback, requests
from supabase import create_client, ClientOptions
from workers.lib.simple_queue import receive, ack
from workers.lib.json_utils import safe_parse_model_json

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
QUEUE = "plans.archive"
TPL = open("/app/prompts/historian.txt").read()


def call_llm(prompt: str) -> dict:
    base = (os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{base}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": os.getenv("RUNPOD_MODEL_NAME", "gpt-4o-mini"),
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 350,
        "temperature": 0,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    data = resp.json()

    raw_text = ""
    try:
        if data.get("choices"):
            choice = data["choices"][0]
            message = choice.get("message") or {}
            raw_text = message.get("content") or message.get("reasoning") or ""
            if not raw_text:
                raw_text = choice.get("text") or ""
    except Exception:
        pass

    if not raw_text:
        raw_text = f"__DEBUG_FULL_RESPONSE__: {json.dumps(data)}"

    parsed, error = safe_parse_model_json(raw_text)
    if error or parsed is None:
        raise ValueError(f"Historian parse error: {error}")
    return parsed


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
