import os, json
from supabase import create_client

SB = create_client(os.getenv("SUPABASE_URL"),
                   os.getenv("SUPABASE_SERVICE_ROLE_KEY"))

def _card(t, col):      # safe stringify
    return json.dumps(t[col] or {}, ensure_ascii=False)

def make_block(thread_id, tier, limit=None):
    q = SB.table("summaries") \
           .select("narrative_summary,abstract_summary") \
           .eq("thread_id", thread_id) \
           .eq("tier", tier) \
           .order("tier_index", ascending=True)
    if limit:
        q = q.limit(limit)
    rows = q.execute().data
    lines = [x for r in rows for x in
             (r["narrative_summary"], r["abstract_summary"])]
    return "\n".join(lines)

def build_prompt(template, thread_id, raw_turns):
    path = f"/app/prompts/{template}.txt"
    tpl  = open(path).read()

    thread = SB.table("threads").select("*") \
                .eq("id", thread_id).single().execute().data

    repl = {
        "{LIFETIME_BLOCK}"        : make_block(thread_id,"lifetime"),
        "{YEAR_BLOCK}"            : make_block(thread_id,"year",6),
        "{SEASON_BLOCK}"          : make_block(thread_id,"season",6),
        "{CHAPTER_BLOCK}"         : make_block(thread_id,"chapter",5),
        "{RAW_TURNS}"             : raw_turns,
        "{FAN_PSYCHIC_CARD}"      : _card(thread,"fan_psychic_card"),
        "{FAN_IDENTITY_CARD}"     : _card(thread,"fan_identity_card"),
        "{CREATOR_PSYCHIC_CARD}"  : _card(thread,"creator_psychic_card"),
        "{CREATOR_IDENTITY_CARD}" : _card(thread,"creator_identity_card"),
    }
    for k,v in repl.items():
        tpl = tpl.replace(k, v)
    return tpl
