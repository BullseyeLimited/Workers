import os, json, time, traceback, requests, re
from supabase import create_client, ClientOptions
from workers.lib.simple_queue import receive, ack

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


def call_llm(prompt: str) -> str:
    base = (os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")

    url = f"{base}/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": os.getenv("RUNPOD_MODEL_NAME", "gpt-oss-20b-uncensored"),
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

    return raw_text


HEADER_PATTERN = re.compile(
    r"^\s*(HORIZON|STATUS|PLAN_SUMMARY|CHANGE_REASON)\s*:\s*(.*)$",
    re.IGNORECASE | re.MULTILINE,
)


def parse_historian_headers(raw_text: str) -> dict:
    """
    Parse header-based historian output into a structured dict.
    Expected headers: HORIZON, STATUS, PLAN_SUMMARY, CHANGE_REASON.
    """
    fields = {
        "HORIZON": "",
        "STATUS": "",
        "PLAN_SUMMARY": "",
        "CHANGE_REASON": "",
    }

    for match in HEADER_PATTERN.finditer(raw_text or ""):
        key = match.group(1).upper()
        value = (match.group(2) or "").strip()
        if key in fields:
            fields[key] = value

    missing = [k for k, v in fields.items() if not v]
    if missing:
        raise ValueError(f"Historian missing fields: {', '.join(missing)}")

    return {
        "horizon": fields["HORIZON"],
        "plan_status": fields["STATUS"],
        "previous_plan_summary": fields["PLAN_SUMMARY"],
        "reason_for_change": fields["CHANGE_REASON"],
    }


def process_job(payload):
    thread_id = payload["thread_id"]
    turn_index = payload.get("turn_index")
    # Skip archiving for the very first turn; ack happens in the caller.
    if isinstance(turn_index, int) and turn_index <= 1:
        print(f"[historian] Skipping archive for thread {thread_id} turn {turn_index} (first turn).", flush=True)
        return
    try:
        turn_count_row = (
            SB.table("threads")
            .select("turn_count")
            .eq("id", thread_id)
            .single()
            .execute()
            .data
            or {}
        )
        if (turn_count_row.get("turn_count") or 0) <= 1:
            # Skip historian on the very first turn.
            return
    except Exception:
        # If we cannot fetch the thread safely, avoid blocking the queue; proceed.
        pass

    prompt = TPL.format(**payload)
    raw_text = call_llm(prompt)
    parsed = parse_historian_headers(raw_text)
    if turn_index is not None:
        parsed["turn_index"] = turn_index

    SB.table("plan_history").insert(
        {
            "thread_id": thread_id,
            "horizon": payload["horizon"],
            "plan_status": payload["plan_status"],
            "previous_plan": payload["previous_plan"],
            "reason_for_change": payload["reason_for_change"],
            "summary_json": json.dumps(parsed, ensure_ascii=False),
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
