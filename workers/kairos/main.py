import os, json, time, requests
from supabase import create_client

SB  = create_client(os.getenv("SUPABASE_URL"),
                    os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
QUEUE = "kairos.analyse"

def runpod_call(prompt):
    url = os.getenv("RUNPOD_URL")
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY')}"}
    body = {"model": "gpt-4o-mini", "prompt": prompt, "max_tokens": 20}
    r = requests.post(url, headers=headers, json=body, timeout=60)
    return r.json()

print("Kairos started — waiting for jobs")
while True:
    # 1 job at a time
    job = SB.rpc("pgmq_receive", params={"q_name": QUEUE, "vt": 30}).data
    if not job:
        time.sleep(2)
        continue

    msg = job["message"]
    message_id = msg["message_id"]

    # fetch raw fan message
    row = SB.table("messages").select("*").eq("id", message_id).single().execute().data
    prompt = f"Dummy prompt: {row['message_text']}"
    llm_json = runpod_call(prompt)

    # write result
    SB.table("message_ai_details").upsert({
        "message_id": message_id,
        "raw_output": llm_json,
        "extract_status": "ok"
    }).execute()

    SB.rpc("pgmq_delete", params={"q_name": QUEUE, "msg_id": job["vt"]})
    print(f"Processed {message_id}")