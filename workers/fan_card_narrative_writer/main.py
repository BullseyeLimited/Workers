import json
import os
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests
from supabase import ClientOptions, create_client

from workers.lib.simple_queue import ack, receive

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

QUEUE = "fan_card.narrative_update"

FAN_CARD_TEMPLATE = """{
  "LONG_TERM": {
    "profile": {
      "identity": { "notes": null },
      "nicknames": { "notes": null },
      "pronouns": { "notes": null },
      "birth": { "notes": null },
      "age_band": { "notes": null },
      "gender": { "notes": null },
      "nationalities": { "notes": null },
      "citizenships": { "notes": null },
      "languages": { "notes": null },
      "timezone": { "notes": null },
      "marital_status": { "notes": null }
    },
    "contact": {
      "addresses": { "notes": null }
    },
    "handles": {
      "social_accounts": { "notes": null },
      "messaging_accounts": { "notes": null },
      "gaming_accounts": { "notes": null },
      "creator_platforms": { "notes": null }
    },
    "appearance": {
      "body": { "notes": null },
      "hair": { "notes": null },
      "eyes": { "notes": null },
      "skin": { "notes": null },
      "marks_scars": { "notes": null },
      "accessories": { "notes": null },
      "sizes": { "notes": null },
      "style_keywords": { "notes": null }
    },
    "residence": {
      "primary": { "notes": null },
      "secondary": { "notes": null },
      "history": { "notes": null }
    },
    "places": {
      "frequent": { "notes": null },
      "favorite": { "notes": null }
    },
    "family": {
      "parents": { "notes": null },
      "guardians": { "notes": null },
      "siblings": { "notes": null },
      "children": { "notes": null },
      "partners": { "notes": null },
      "past_partners": { "notes": null },
      "in_laws": { "notes": null },
      "extended": { "notes": null },
      "household": { "notes": null }
    },
    "social": {
      "friends": { "notes": null },
      "close_contacts": { "notes": null },
      "colleagues": { "notes": null },
      "mentors": { "notes": null },
      "neighbors": { "notes": null },
      "community": { "notes": null }
    },
    "pets": {
      "records": { "notes": null }
    },
    "education": {
      "institutions": { "notes": null },
      "degrees": { "notes": null },
      "fields_of_study": { "notes": null },
      "graduation_years": { "notes": null },
      "certifications": { "notes": null },
      "awards": { "notes": null }
    },
    "work": {
      "employers": { "notes": null },
      "titles": { "notes": null },
      "departments": { "notes": null },
      "teams": { "notes": null },
      "supervisors": { "notes": null },
      "direct_reports": { "notes": null },
      "colleagues": { "notes": null },
      "clients": { "notes": null },
      "projects": { "notes": null },
      "locations": { "notes": null },
      "contracts": { "notes": null },
      "professional_memberships": { "notes": null },
      "unions": { "notes": null }
    },
    "memberships": {
      "gyms": { "notes": null },
      "clubs": { "notes": null },
      "loyalty_programs": { "notes": null },
      "media_subscriptions": { "notes": null },
      "volunteering": { "notes": null }
    },
    "devices": {
      "phones": { "notes": null },
      "computers": { "notes": null },
      "tablets": { "notes": null },
      "consoles": { "notes": null },
      "wearables": { "notes": null },
      "smart_home": { "notes": null },
      "cameras": { "notes": null }
    },
    "services": {
      "mobile_carriers": { "notes": null },
      "isp": { "notes": null },
      "email_providers": { "notes": null },
      "cloud_storage": { "notes": null },
      "payment_services": { "notes": null }
    },
    "transport": {
      "vehicles": { "notes": null },
      "driver_licenses": { "notes": null },
      "insurance": { "notes": null },
      "commute_modes": { "notes": null },
      "transit_cards": { "notes": null }
    },
    "health": {
      "allergies": { "notes": null },
      "dietary": { "notes": null },
      "chronic_conditions": { "notes": null },
      "medications_regular": { "notes": null },
      "providers": { "notes": null },
      "accessibility": { "notes": null }
    },
    "travel": {
      "history": { "notes": null },
      "favorite_destinations": { "notes": null },
      "visited_countries": { "notes": null }
    },
    "favorites": {
      "hobbies": { "notes": null },
      "sports": { "notes": null },
      "teams": { "notes": null },
      "games": { "notes": null },
      "music": { "notes": null },
      "podcasts": { "notes": null },
      "movies": { "notes": null },
      "tv": { "notes": null },
      "books": { "notes": null },
      "authors": { "notes": null },
      "cuisine": { "notes": null },
      "foods": { "notes": null },
      "drinks": { "notes": null },
      "restaurants": { "notes": null },
      "colors": { "notes": null },
      "fashion": { "notes": null },
      "brands": { "notes": null },
      "accessories": { "notes": null },
      "cosmetics": { "notes": null },
      "places": { "notes": null },
      "travel": { "notes": null },
      "animals": { "notes": null },
      "flowers": { "notes": null },
      "creators": { "notes": null },
      "influencers": { "notes": null },
      "channels": { "notes": null }
    },
    "accounts": {
      "social_platforms": { "notes": null },
      "gaming_platforms": { "notes": null },
      "streaming_platforms": { "notes": null },
      "community_platforms": { "notes": null }
    },
    "contacts": {
      "emergency": { "notes": null },
      "key_contacts": { "notes": null }
    }
  },

  "SHORT_TERM": {
    "location": {
      "current": { "notes": null },
      "venue": { "notes": null }
    },
    "travel": {
      "current_trip": { "notes": null },
      "accommodation": { "notes": null }
    },
    "activity": {
      "current": { "notes": null }
    },
    "company": {
      "present_people": { "notes": null }
    },
    "availability": {
      "status": { "notes": null }
    },
    "device": {
      "current": { "notes": null }
    },
    "connection": {
      "status": { "notes": null }
    },
    "transport": {
      "mode": { "notes": null },
      "route": { "notes": null }
    },
    "schedule": {
      "current_event": { "notes": null },
      "current_meeting": { "notes": null },
      "current_task": { "notes": null },
      "next_appointments": { "notes": null },
      "upcoming_deadlines": { "notes": null },
      "reminders_today": { "notes": null }
    },
    "interactions": {
      "recent_calls": { "notes": null },
      "recent_messages": { "notes": null }
    },
    "places": {
      "recent_visits": { "notes": null }
    },
    "media": {
      "now_playing": { "notes": null },
      "now_watching": { "notes": null },
      "now_reading": { "notes": null },
      "recent_published": { "notes": null }
    },
    "health": {
      "current_issue": { "notes": null },
      "symptoms": { "notes": null },
      "recent_sleep": { "notes": null },
      "recent_exercise": { "notes": null },
      "recent_meals": { "notes": null },
      "temp_injury": { "notes": null },
      "medication_change": { "notes": null }
    },
    "appearance": {
      "temp_change": { "notes": null }
    },
    "residence": {
      "temporary": { "notes": null }
    },
    "work": {
      "temporary_assignment": { "notes": null }
    },
    "relationships": {
      "recent_update": { "notes": null }
    },
    "devices": {
      "temp_issue": { "notes": null }
    },
    "tasks": {
      "open_promises": { "notes": null }
    },
    "environment": {
      "weather": { "notes": null },
      "event_context": { "notes": null },
      "queue_wait": { "notes": null }
    }
  }
}"""

_TEMPLATE_DICT = json.loads(FAN_CARD_TEMPLATE)

RUNPOD_URL = os.getenv("RUNPOD_URL", "").rstrip("/")
RUNPOD_MODEL_NAME = os.getenv("RUNPOD_MODEL_NAME", "your-default-model-name")


PROMPT_TEMPLATE = """
 You are ARCHIVIST, the memory compression layer. 
Two identity cards exist, FAN and CREATOR. This call concerns the FAN card.
CARD PHILOSOPHY
The Fan identity card has two writable zones.
LONG TERM means stable facts that the fan will probably keep for months or years.

WORKING means short window facts that are likely to change or expire within about thirty days.

Your responsibility in every call is:
Scan the 20 new turns and decide whether there are any fan facts worth storing.

For each fact that is worth storing, classify it as LONG TERM or WORKING.

Map the fact to the single most specific top level category from the Appendix.

Create or extend a structured JSON entry with subcategories and notes under that category in the correct zone.

Sometimes a batch of 20 turns will contain no useful storable facts.
Sometimes it will contain many important facts.
Both are normal and you must handle both correctly.
IDENTITY CARD STRUCTURE
The identity card is a nested JSON object with two zones named long_term and working.
long_term holds durable facts that last for months or years.
working holds short lived facts that are likely to change within about one month.
Each zone contains top level categories.
Each top level category name must come from the Appendix list.
Each category contains entities and possibly sub entities, all expressed as JSON objects.
High level shape of the delta output in words:
At the top level, you may have the key long_term and or the key working.

Under long_term you store only categories that gained new or extended entities in this call.

Under working you store only categories that gained new or extended entities in this call.

Under each category you store entity keys.

Each entity is an object that always has a notes field and may have other fields.

Any sub object you create must also have its own notes field.

A category name must be one of the valid top level categories from the Appendix such as family.siblings, devices.phones, st.location.current and so on.
An entity key is a stable identifier that you invent for a specific entity such as
 self
 sister_julia
 mom
 phone_iphone_15
 current_trip_paris_2025
Every JSON object you create at any depth, whether entity or sub entity, must have a notes field containing a short meaningful summary in as few words as possible.
You output only the delta, meaning only new or extended structures, not the entire card.
WHAT YOU RECEIVE, IMMUTABLE INPUT SCHEMA
You receive two inputs.
First input named raw_turns.
 raw_turns is a list with exactly twenty items.
 Each item represents one turn in the conversation and has at least two fields.
 Field speaker whose value is either fan or creator.
 Field text which contains the full turn text.
Second input named fan_identity_card_current.
 fan_identity_card_current is the full JSON snapshot of the current FAN identity card.
You must treat fan_identity_card_current as the only source of truth about
which entities and sub entities already exist

what subcategory and sub subcategory patterns already exist

how entities are currently keyed and structured

You must
use fan_identity_card_current to avoid duplicates

use its existing nested structure and naming patterns as your guide for subcategories

extend this structure when you add new entities or sub entities, there is no other schema

SCOPE OF FAN FACT
Treat something as a fan fact only if it is a concrete factual statement about one of these areas.
About the fan
age, birth, residence, jobs, roles, schedules

appearance, possessions, devices, memberships

education, work history, commute, stable habits

About relatives
parents, guardians, siblings, children, partners, former partners, in laws, extended family, household members

About the social graph
friends, colleagues, mentors, neighbors, community members

Also in scope
pets

devices, services, transport, memberships

travel history and destinations

health such as allergies, chronic conditions, providers, accessibility

current context and state, which usually belongs to the WORKING zone, for example
 where they are now, what they are doing, who they are with
 current device and connection status
 current transport mode or route
 schedule in the near future
 recent calls or messages, places, media
 current health issues, recent sleep, exercise, meals
 temporary changes in residence, relationships, work, devices, and so on

If the fan mentions someone else only in relation to themself, for example the sentence my friend Julia lives in Toronto, then
store Julia under the relevant category such as social.friends

create or extend a subcategory for that entity, for example friend_julia

capture the concrete facts that were stated

You must ignore
abstract feelings, opinions, preferences, moods

purely emotional content, flirting style, jokes

hypothetical or speculative scenarios that have no clear factual ground

Those belong to a different card and not to this one.
DURABILITY RULE, ZONE DECISION
For each atomic fact that you detect, decide its durability.
Facts that are likely stable for more than one month must go to the long_term zone.
 Examples include hometown, degree, employer, city where a sibling lives, chronic condition, favorite team, phone model owned.
Facts that are likely to change within about one month must go to the working zone.
 Examples include current city for a trip, current meeting, current device in use, this week schedule, today tasks, currently sick, staying at moms place this week.
If you are unsure
prefer the long_term zone for identity level or history type facts

prefer the working zone for live context facts, things happening right now, this week, or explicitly temporary states

The choice of zone, long_term or working, is how you express whether a fact is long term or short term.
 Do not add any extra labels for this.
Sometimes all detected facts in a batch will be long_term.
 Sometimes all will be working.
 Sometimes there will be none.
 You must classify accurately and not force both zones.
ENTITY GROUPING AND STRUCTURE
You do not simply append flat strings.
 You must build or extend structured entities inside the correct category.
Step one
 For each fact, choose the most specific top level category from the Appendix.
 For example, the sentence my sister lives in Toronto belongs to the category family.siblings.
Step two
 Under that category, group facts by entity.
If the entity already exists in fan_identity_card_current under this category, reuse its key and follow its existing structure.

If the entity is new, invent a concise stable key such as
 sister
 sister_julia
 brother_older
 friend_julia_toronto

Step three
 For each entity key, create or extend a JSON object.
 This object always includes a notes field and may include more fields or nested objects.
 Each nested object must also have its own notes field.
For example, under a sibling entity you might create sub objects such as
location with notes like lives in toronto

kids with notes like has two kids, and child sub entities such as
 child_1 with notes older child seven years old
 child_2 with notes younger child four years old

Step four
 Every object you create, whether entity or sub entity, must contain a notes field with a very short summary.
 The notes text should use as few words as possible but still capture the key meaning.
Subcategories and sub subcategories have no predefined list outside of fan_identity_card_current.
You must base naming and structure choices on
patterns that already exist in fan_identity_card_current

the concrete facts present in raw_turns

When you need to represent a genuinely new entity or attribute that has no existing pattern, you must
invent a clear intuitive new key such as trip, device_model, school, location

treat that new key as part of the evolving schema of the fan card from that point onward

You must not assume any hidden or external schema for subcategories beyond
the Appendix list for top level categories

the actual JSON structure that already exists in fan_identity_card_current

WHAT COUNTS AS WORTH STORING
 From the twenty turns, not every sentence is worth storing.
Store a fact only if
it is factual and concrete

it is about the fan, their close graph, or their environment or state

it could plausibly matter later, for example facts about identity, relationships, health, travel, devices, schedule, and similar areas

Skip facts that are
small one off details with no persistent relevance, for example I had cereal this morning, unless clearly important in context

pure jokes, sarcasm, fiction, or speculative statements

ambiguous, where you cannot confidently extract a concrete fact

It is valid and expected that
some batches will produce no storable facts, in that case you must output an empty JSON object

other batches will produce many storable facts across multiple categories and zones

RULES ABOUT EXISTING DATA
 You must never delete or modify any existing fact in either zone.
 You only append new information.
You may append
new entities

new sub entities

genuinely new details for existing entities

Before adding anything, always check fan_identity_card_current.
If an equivalent entity and detail already exist with the same meaning, skip it.

If the new detail is more specific, for example lives in Toronto Canada when you previously only had Toronto, then you may append the new detail.

Avoid duplicates.
If an entity key and a detail already exist, do not repeat them.

Use case insensitive comparison on strings when checking for equivalence.

HOW TO THINK, INTERNAL WORKFLOW
 Follow this mental workflow.
Read all twenty raw_turns quickly to understand the context.

Focus only on items where the speaker field is fan.

From those turns, highlight sentences that contain concrete factual claims.

Split each compound claim into atomic facts.

For each atomic fact do the following steps.

Step 5a
 Decide if the fact is worth storing, using the rules from the section on what counts as worth storing.
 If it is not worth storing, ignore it.
Step 5b
 Choose the single most specific top level category from the Appendix.
Step 5c
 Identify the entity that the fact belongs to, for example self, sibling, partner, friend, device, trip, and so on.
Step 5d
 Check fan_identity_card_current for this category and entity.
If the entity exists, plan to extend it using the existing nested shape.

If the entity does not exist, create a new entity key and nested shape.

Step 5e
 Apply the durability rule and select long_term or working.
Step 5f
 Design the nested JSON object for this category, entity, and sub entity.
Ensure each object you create has a notes field with a short meaningful summary.

Add only new details and do not overwrite existing content.

When you are done, assemble the JSON delta output.

Include the long_term key only if you have at least one new or extended entity in the long_term zone.

Include the working key only if you have at least one new or extended entity in the working zone.

Within each zone, include only the categories that you touched in this call.

Within each category, include only the entities that you touched in this call.

Validate that the JSON you are about to output is valid.

No trailing commas

All keys are valid JSON strings

The overall structure is a valid JSON object

STYLE AND CONTENT RULES FOR NOTES
 These rules apply when you generate the JSON delta payload.
You must output JSON only for the delta payload, with no extra prose, headings, or explanation.
 Every JSON object you create must have a notes field.
The notes field must follow these rules.
notes text is concise

notes text is lowercased unless it contains proper names

notes text uses as few words as possible while still being meaningful

notes text has no trailing period

Use ISO date format such as YYYY dash MM dash DD when a date is explicit.
 Use common units such as cm, kg, dollar, euro when present in the source text.
 Do not speculate or invent facts.
 Include only what is explicitly stated or clearly implied.
 Place different attributes of the same entity in separate fields or sub objects within the same entity object.
OUTPUT CONTRACT, MACHINE READABLE FORM
 Your response to the orchestrator must be a single JSON object shaped as follows, described in words.
At the top level there may be a long_term object and or a working object.

Under long_term each key is a category name from the Appendix, and each value is an object of entities.

Under working each key is a category name from the Appendix, and each value is an object of entities.

Under each category, each key is an entity_key and each value is an object that contains at least a notes field and may contain other fields or nested objects.

Every nested object you create must have its own notes field.

You must omit the long_term key entirely if there are no new long term facts.
 You must omit the working key entirely if there are no new working or short term facts.
 If there are no new facts at all, return an empty JSON object.
A category name must be exactly one of the valid top level category names from the Appendix.
Subcategories and sub subcategories
must be derived from the existing nested structure in fan_identity_card_current

may also include new entities or fields that you introduce in this call based on the content of raw_turns

==================================================
CURRENT FAN IDENTITY CARD SNAPSHOT
{CURRENT_CARD}

RAW TURNS (exactly last twenty)
{CONVERSATION_TEXT}

FINAL INSTRUCTION
Return only the update blocks or NO_UPDATES. Do not add any extra prose, markdown, or explanation.
""".strip()


def build_prompt(current_card: str, conversation_text: str) -> str:
    """
    Build the archivist prompt while safely injecting dynamic fields.
    """
    return (
        PROMPT_TEMPLATE.replace("{CURRENT_CARD}", current_card or "[none yet]")
        .replace(
            "{CONVERSATION_TEXT}",
            conversation_text or "[no conversation available]",
        )
    )

def _write_debug_logs(system_prompt: str, user_prompt: str, response_payload: dict, response_text: str) -> None:
    """
    Persist prompts and responses to a timestamped folder under ./logs for local debugging.
    Uses a filesystem-safe timestamp (HH-MM,DD.MM) for the folder name.
    """
    try:
        timestamp = datetime.now().strftime("%H-%M,%d.%m")
        base_dir = Path.cwd() / "logs" / timestamp
        base_dir.mkdir(parents=True, exist_ok=True)

        (base_dir / "system_prompt.txt").write_text(system_prompt, encoding="utf-8")
        (base_dir / "user_prompt.txt").write_text(user_prompt, encoding="utf-8")
        (base_dir / "response_text.txt").write_text(response_text or "", encoding="utf-8")
        (base_dir / "response_payload.json").write_text(
            json.dumps(response_payload or {}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:  # noqa: BLE001
        print("[fan_card_narrative_writer] Failed to write debug logs:", exc, flush=True)

def runpod_call(system_prompt: str, user_prompt: str) -> str:
    # Use chat/completions with explicit system/user split.
    url = "http://213.173.96.53:10320/v1/chat/completions"
    body = {
        "model": "/workspace/models/qwen3-235b-instruct-q4km/Q4_K_M.gguf",
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "max_tokens": 4096,
        "temperature": 0.4,
        "top_p": 1.0,
        "top_k": 1,
        "min_p": 0.05,
        "repeat_penalty": 1.05,
    }
    # Structured logging of the outbound request (excluding secrets).
    try:
        payload_preview = json.dumps(body, ensure_ascii=False, separators=(",", ":"))
        print(
            "[fan_card_narrative_writer] RunPod request meta:",
            json.dumps({k: body[k] for k in body if k != "messages"}),
        )
        print(
            "[fan_card_narrative_writer] RunPod payload bytes:",
            len(payload_preview.encode("utf-8")),
        )
        print("[fan_card_narrative_writer] RunPod URL:", url)
        print("[fan_card_narrative_writer] RunPod system prompt start")
        print(system_prompt)
        print("[fan_card_narrative_writer] RunPod system prompt end")
        print("[fan_card_narrative_writer] RunPod user prompt start")
        print(user_prompt)
        print("[fan_card_narrative_writer] RunPod user prompt end")
    except Exception:
        pass

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "Authorization": f"Bearer {os.getenv('RUNPOD_API_KEY', '')}",
    }
    resp = requests.post(url, headers=headers, json=body, timeout=240)
    # Log response status and text for debugging.
    try:
        print(
            "[fan_card_narrative_writer] RunPod response",
            json.dumps({"status": resp.status_code, "text": resp.text}),
        )
    except Exception:
        pass
    resp.raise_for_status()
    data = resp.json()
    choice = data["choices"][0]
    text = (choice.get("message") or {}).get("content") or ""

    # Persist debug artifacts locally to inspect prompts and responses.
    _write_debug_logs(system_prompt, user_prompt, data, text)
    return text.strip()


def build_conversation_slice(messages: list[dict]) -> str:
    lines: list[str] = []
    for msg in messages:
        role = "F" if msg.get("sender") == "fan" else "C"
        lines.append(f"[{role} turn {msg.get('turn_index')}] {msg.get('message_text', '').strip()}")
    return "\n".join(lines)


def safe_load_card(card_text: str) -> dict:
    if not card_text:
        return {}
    try:
        data = json.loads(card_text)
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def ensure_card_root(card: dict) -> dict:
    if not isinstance(card, dict):
        card = {}
    long_term = card.get("long_term")
    if not isinstance(long_term, dict):
        long_term = card.get("LONG_TERM") if isinstance(card.get("LONG_TERM"), dict) else {}
    short_term = card.get("short_term")
    if not isinstance(short_term, dict):
        if isinstance(card.get("SHORT_TERM"), dict):
            short_term = card.get("SHORT_TERM")
        elif isinstance(card.get("working"), dict):
            short_term = card.get("working")
        else:
            short_term = {}
    card["long_term"] = long_term
    card["short_term"] = short_term
    card.pop("LONG_TERM", None)
    card.pop("SHORT_TERM", None)
    card.pop("working", None)
    return card


def default_card_structure() -> dict:
    long_term = _TEMPLATE_DICT.get("LONG_TERM", {})
    short_term = _TEMPLATE_DICT.get("SHORT_TERM", {})
    return {
        "long_term": json.loads(json.dumps(long_term)),
        "short_term": json.loads(json.dumps(short_term)),
    }


def merge_cards(base: dict, delta: dict) -> dict:
    for key, value in delta.items():
        if isinstance(value, dict):
            base_value = base.get(key)
            if isinstance(base_value, dict):
                merge_cards(base_value, value)
            else:
                base[key] = json.loads(json.dumps(value))
        else:
            base[key] = value
    return base


def process_job(payload: dict) -> None:
    thread_id = payload.get("thread_id")
    payload_turn = payload.get("fan_turn_index")

    # Pull the latest fan turn directly from the DB so we are not tied to an external counter.
    latest_fan_turn = None
    if thread_id:
        latest_res = (
            SB.table("messages")
            .select("turn_index")
            .eq("thread_id", thread_id)
            .eq("sender", "fan")
            .order("turn_index", desc=True)
            .limit(1)
            .execute()
        )
        latest_row = (latest_res.data or [None])[0] or {}
        turn_val = latest_row.get("turn_index")
        if isinstance(turn_val, int) and turn_val > 0:
            latest_fan_turn = turn_val

    fan_turn_index = latest_fan_turn if latest_fan_turn else payload_turn

    if (
        not thread_id
        or not isinstance(fan_turn_index, int)
        or fan_turn_index <= 0
    ):
        print(
            "[fan_card_narrative_writer] Missing or invalid thread_id/fan_turn_index in payload, skipping.",
            flush=True,
        )
        return

    res = (
        SB.table("threads")
        .select("fan_identity_card")
        .eq("id", thread_id)
        .single()
        .execute()
    )
    thread = res.data
    if not thread:
        print(
            f"[fan_card_narrative_writer] Thread {thread_id} not found, skipping update.",
            flush=True,
        )
        return

    current_card_value = thread.get("fan_identity_card")
    if isinstance(current_card_value, str):
        current_card_raw = current_card_value.strip()
    elif isinstance(current_card_value, dict):
        current_card_raw = json.dumps(current_card_value, ensure_ascii=False)
    else:
        current_card_raw = ""
    parsed_card = safe_load_card(current_card_raw)
    card_was_empty = not parsed_card
    if card_was_empty:
        current_card = default_card_structure()
        current_card_raw = json.dumps(current_card, ensure_ascii=False)
        # Persist a seeded empty card immediately so the thread always has a JSON object.
        SB.table("threads").update({"fan_identity_card": current_card}).eq(
            "id", thread_id
        ).execute()
    else:
        current_card = ensure_card_root(parsed_card)

    start_turn = max(1, fan_turn_index - 19)
    msgs_res = (
        SB.table("messages")
        .select("sender,message_text,turn_index")
        .eq("thread_id", thread_id)
        .gte("turn_index", start_turn)
        .lte("turn_index", fan_turn_index)
        .order("turn_index", desc=False)
        .execute()
    )
    messages = msgs_res.data or []
    conversation_text = build_conversation_slice(messages)

    prompt = build_prompt(current_card_raw, conversation_text)

    split_marker = "CURRENT FAN IDENTITY CARD SNAPSHOT"
    idx = prompt.find(split_marker)
    if idx != -1:
        system_prompt = prompt[:idx].strip()
        user_prompt = prompt[idx:].strip()
    else:
        system_prompt = prompt
        user_prompt = "Return only the JSON delta payload."

    new_card_text = runpod_call(system_prompt, user_prompt).strip()

    delta_card = ensure_card_root(safe_load_card(new_card_text))
    if not delta_card["long_term"] and not delta_card["short_term"]:
        print(
            "[fan_card_narrative_writer] Model returned no JSON delta; skipping update.",
            flush=True,
        )
        return

    merged_card = ensure_card_root(current_card)
    merge_cards(merged_card, delta_card)

    # Persist the delta snapshot for observability.
    def _log_delta():
        try:
            if not delta_card.get("long_term") and not delta_card.get("short_term"):
                return
            SB.table("fan_card_logs").insert(
                {
                    "thread_id": thread_id,
                    "fan_turn_index": fan_turn_index,
                    "delta_json": delta_card,
                }
            ).execute()
        except Exception as exc:  # noqa: BLE001
            print("[fan_card_narrative_writer] Failed to log fan_card delta:", exc, flush=True)

    _log_delta()

    SB.table("threads").update({"fan_identity_card": merged_card}).eq(
        "id", thread_id
    ).execute()

    print(
        "[fan_card_narrative_writer] Updated fan_identity_card "
        f"for thread {thread_id} at fan turn {fan_turn_index}",
        flush=True,
    )


def main() -> None:
    while True:
        # RunPod calls can run for a couple of minutes; keep the visibility timeout
        # longer than the worst-case call so the same job is not picked up twice by
        # another worker instance.
        job = receive(QUEUE, 600)
        if not job:
            time.sleep(1)
            continue

        row_id = job["row_id"]
        payload = job["payload"]
        try:
            process_job(payload)
        except Exception as exc:  # noqa: BLE001
            print("[fan_card_narrative_writer] Error:", exc, flush=True)
            traceback.print_exc()
        finally:
            ack(row_id)


if __name__ == "__main__":
    main()
