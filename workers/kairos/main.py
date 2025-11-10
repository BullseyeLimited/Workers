import os, time
from supabase import create_client
from lib.prompt_builder import build_prompt

SB = create_client(os.getenv("SUPABASE_URL"),
                   os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
QUEUE = "kairos.analyze"

def pop_job():
    row = SB.rpc("pgmq_pop", {"queue_name": QUEUE, "vt": 30}).execute().data
    return row[0] if row else None

def ack(msg_id):
    SB.rpc("pgmq_complete", {"queue_name": QUEUE, "msg_id": msg_id}).execute()

def run_once():
    j = pop_job()
    if not j:
        return False
    payload = j["payload"]              # {message_id:…}
    msg = SB.table("messages").select("*") \
             .eq("id", payload["message_id"]).single().execute().data
    # last 40 turns
    turns = SB.table("messages") \
              .select("sender,message_text,turn_index") \
              .eq("thread_id", msg["thread_id"]) \
              .gte("turn_index", msg["turn_index"]-39) \
              .order("turn_index").execute().data
    raw = "\n".join([f'{t["sender"].title()}: {t["message_text"]}' for t in turns])
    prompt = build_prompt("kairos", msg["thread_id"], raw)
    print("\n--- Kairos prompt sample ---\n", prompt[:600], "\n--- end ---")
    ack(j["id"])
    return True

if __name__ == "__main__":
    while True:
        if not run_once():
            time.sleep(1)
