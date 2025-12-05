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

HARD EXCLUSIONS -- NEVER STORE
- Conversation meta about this call itself (how the chat feels, tone, style, sexting, jokes, compliments, roleplay content, message contents), including "we are sexting," "you called me babe," etc.
- Forms of address, pet names, nicknames, or honorifics used for the Creator (e.g., "babe," "sir," "princess") -- these are not entities.
- Any fact whose only source is a Creator turn, unless the fan explicitly affirms it as their own factual statement.
- Inferences about intent, mood, initiation, intensity, or style (e.g., who started the chat, whether it was explicit, playful, etc.).
- Any information about the Creator as a person or contact. The Creator is **out of scope** for the FAN card.

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

TEXT_SCRIBE_SYSTEM_PROMPT = """SYSTEM: You are TEXT_DELTA_SCRIBE (FAN). You convert a FAN identity-card JSON delta into a plain-text delta that can be appended to an existing plain-text identity card.

YOU RECEIVE (3 INPUTS)
1) fan_identity_card_current_json
- Full current FAN card JSON (context only; NOT a source of new facts)

2) fan_identity_card_delta_json
- The new JSON delta (THIS is the ONLY source of facts you may write)

3) fan_identity_card_current_text
- The existing plain-text identity card (for deduplication only)

NON-NEGOTIABLE RULES
- Output plain text only. Never output JSON.
- Write ONLY facts present in fan_identity_card_delta_json (including its key names / paths). Do not add, infer, or assume anything.
- You may consult fan_identity_card_current_json ONLY for pronoun style (he/she/they) and naming consistency, but you MUST NOT output any fact unless it appears in the delta.
- You may consult fan_identity_card_current_text ONLY to prevent duplicates.
- If there is nothing to output after dedupe, output exactly:
  EMPTY

ZONES
The delta may use either:
- Style A: "LONG_TERM" and "SHORT_TERM"
- Style B: "long_term" and "working"

Normalize output to TWO section headers (only include if they have lines):
LONG_TERM
SHORT_TERM

Map:
- LONG_TERM or long_term  -> LONG_TERM section
- SHORT_TERM or working   -> SHORT_TERM section

OUTPUT FORMAT (MUST BE PARSEABLE)
Within each section, output one fact per line, in this exact line grammar:
<FACT_KEY> :: <FACT_TEXT>

FACT_KEY SPEC (STABLE POINTERS)
Use JSON-pointer-style paths (slash-separated) so dotted keys are safe.
- LONG_TERM facts MUST use prefix: LT:
- SHORT_TERM facts MUST use prefix: ST:
FACT_KEY = "<LT: or ST:>" + "<json pointer path from that zone root to the owner of the fact>"

Owner rules:
- For a notes-based fact: the owner is the object that contains the non-empty "notes" field. Do NOT include "/notes" in FACT_KEY.
- For a primitive leaf fact: the owner is that primitive value itself (use its exact pointer path).
Keep keys exactly as they appear in the delta JSON (preserve underscores, casing, digits, etc).

WHAT TO EXTRACT FROM THE DELTA (DELTA-ONLY)
Traverse fan_identity_card_delta_json recursively (depth-first or any method is fine as long as output ordering rules are applied later).
Emit a candidate fact whenever you find:
- an object with a non-empty string field named "notes"
- a primitive leaf value (string/number/boolean) that is not null

Skip:
- notes that are null / empty / whitespace-only
- null values
- empty objects/containers with no printable facts beneath them

FACT_TEXT (CLEAR ENGLISH REWRITE; NO NEW FACTS)
If an orchestrator provides an explicit template/mapping block at runtime, apply it.
Otherwise use the following deterministic fallback rules.

A) NOTES-BASED FACTS ("notes" string)
Goal: rewrite into a clean, grammatical English sentence while preserving meaning. You may ONLY do:
- casing fixes
- punctuation fixes
- minimal glue words (“is/are/was”, articles, prepositions)
- reordering for readability
You MUST NOT introduce any new details.

Subject handling:
- If the notes already contain an explicit subject (e.g., “Julia is 19”), keep it (only minor cleanup).
- If the notes are a fragment lacking a clear subject (e.g., “age 19”, “lives in Toronto”), you MAY prepend a subject derived from the FACT_KEY path using the KEY→LABEL rules below.
- If even then it still doesn’t form a sentence safely, output a minimally-edited version of the notes as the sentence.

Common mini-normalizations allowed (examples, not exhaustive):
- “age 19” -> “is 19 years old”
- “born 2004” -> “was born in 2004”
- “in prague” -> “is in Prague”
Do NOT guess missing verbs/subjects beyond the KEY→LABEL subject.

B) PRIMITIVE LEAF FACTS (string/number/boolean)
Goal: output clear English without adding meaning.
- Preferred: “<Label> is <Value>.”
- If the value already reads like a sentence, you may keep it as-is.
- Booleans: use “<Label> is true/false.”

KEY→LABEL (DETERMINISTIC, DELTA-DERIVED)
This is used ONLY to create a subject/label from the FACT_KEY path.
Choose a raw identifier from the FACT_KEY path:
- Let segments = the pointer segments after the zone root (e.g., /family/siblings/sister_julia -> ["family","siblings","sister_julia"])
- Default raw_id = last segment.
- If raw_id is an array index like "0" or "1", set raw_id = (second-to-last segment) + " " + index.

Convert raw_id to a readable base label:
- Replace '_' and '-' with spaces.
- Split camelCase boundaries.
- Collapse repeated whitespace.
- Keep digits as-is.
- BaseLabel = Title Case each word EXCEPT small glue words ("of","and","or","in","on","at","to","for","with","a","an","the") which should be lowercased unless first word.

Relationship-pattern upgrade (makes “Sister Julia”, “Best friend Max”, etc):
Prefixes (first match wins): best_friend, close_friend, ex_partner, step_mother, step_father, sister, brother, mother, father, parent, spouse, wife, husband, partner, child, son, daughter, friend.
RelationPhrase formatting:
- "best_friend" -> "Best friend"
- "close_friend" -> "Close friend"
- "ex_partner" -> "Ex-partner"
- "step_mother" -> "Step-mother"
- "step_father" -> "Step-father"
- single-word relations -> Title Case
NamePart = remaining tokens Title Cased and joined with spaces. If none, use the RelationPhrase alone.
Final Label = RelationPhrase + NamePart if matched, else BaseLabel.

PRONOUNS (OPTIONAL)
- Prefer Label-based sentences (no pronouns). If you use a pronoun, you may look it up from fan_identity_card_current_json; otherwise use singular “they.”

PUNCTUATION RULE
- Ensure FACT_TEXT ends with ".", "!" or "?"; append "." if missing.

DEDUPLICATION (APPEND-SAFE)
Before outputting a line, skip it if the same information already exists in fan_identity_card_current_text.
Normalize for comparison:
- lowercase
- trim
- collapse whitespace
- remove trailing punctuation
If normalized(candidate_line) is a substring of normalized(current_text), skip it.

ORDERING (DETERMINISTIC)
Within each section, sort emitted lines by FACT_KEY (case-insensitive). Only print a section header if it has at least one line. If no sections have lines, output EXACTLY "EMPTY".
""".strip()

PLAIN_TEXT_CARD_TEMPLATE = """LONG_TERM
family:
social:
pets:
profile:
contact:
handles:
appearance:
residence:
places:
education:
work:
memberships:
devices:
services:
transport:
health:
travel:
favorites:
accounts:
contacts:

SHORT_TERM
location:
travel:
activity:
company:
availability:
device:
connection:
transport:
schedule:
interactions:
places:
media:
health:
appearance:
residence:
work:
relationships:
devices:
tasks:
environment:
""".strip()

# Main categories for quick mapping/ordering
LONG_TERM_CATEGORIES = [
    "family",
    "social",
    "pets",
    "profile",
    "contact",
    "handles",
    "appearance",
    "residence",
    "places",
    "education",
    "work",
    "memberships",
    "devices",
    "services",
    "transport",
    "health",
    "travel",
    "favorites",
    "accounts",
    "contacts",
]

SHORT_TERM_CATEGORIES = [
    "location",
    "travel",
    "activity",
    "company",
    "availability",
    "device",
    "connection",
    "transport",
    "schedule",
    "interactions",
    "places",
    "media",
    "health",
    "appearance",
    "residence",
    "work",
    "relationships",
    "devices",
    "tasks",
    "environment",
]


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

def _log_raw_delta(raw_text: str, thread_id: int, fan_turn_index: int, label: str) -> None:
    """
    Persist raw model output locally for debugging, even if it fails validation.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        base_dir = Path.cwd() / "logs" / "fan_card_raw"
        base_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{timestamp}-thread{thread_id}-turn{fan_turn_index}-{label}.txt"
        (base_dir / fname).write_text(raw_text or "", encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        print("[fan_card_narrative_writer] Failed to log raw delta:", exc, flush=True)

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


def _log_text_delta(raw_text: str, thread_id: int, fan_turn_index: int) -> None:
    """
    Persist the plain-text delta output for observability.
    """
    try:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        base_dir = Path.cwd() / "logs" / "fan_card_text_delta"
        base_dir.mkdir(parents=True, exist_ok=True)
        fname = f"{timestamp}-thread{thread_id}-turn{fan_turn_index}-text-delta.txt"
        (base_dir / fname).write_text(raw_text or "", encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        print("[fan_card_narrative_writer] Failed to log text delta:", exc, flush=True)

def runpod_call(system_prompt: str, user_prompt: str) -> str:
    # Use chat/completions with explicit system/user split.
    base = (os.getenv("RUNPOD_URL") or "").rstrip("/")
    if not base:
        raise RuntimeError("RUNPOD_URL is not set")
    url = f"{base}/v1/chat/completions"
    model = os.getenv(
        "RUNPOD_MODEL_NAME",
        "/workspace/models/qwen3-235b-instruct-q4km/Q4_K_M.gguf",
    )
    body = {
        "model": model,
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


def build_text_scribe_prompt(
    current_card_json: dict, delta_card: dict, current_plain_text: str
) -> str:
    """
    Build the user prompt for TEXT_DELTA_SCRIBE with current card, delta, and existing text.
    """
    return (
        "fan_identity_card_current_json:\n"
        f"{json.dumps(current_card_json or {}, ensure_ascii=False)}\n\n"
        "fan_identity_card_delta_json:\n"
        f"{json.dumps(delta_card or {}, ensure_ascii=False)}\n\n"
        "fan_identity_card_current_text:\n"
        f"{current_plain_text or ''}\n"
    )


def _init_plain_text_state() -> dict:
    """
    Return an empty structure keyed by section -> category -> list of lines.
    """
    return {
        "LONG_TERM": {cat: [] for cat in LONG_TERM_CATEGORIES},
        "SHORT_TERM": {cat: [] for cat in SHORT_TERM_CATEGORIES},
    }


def _parse_plain_text_card(text: str) -> dict:
    """
    Parse the existing plain-text card into section/category buckets.
    Expects headers LONG_TERM/SHORT_TERM and category lines ending with ':'.
    """
    state = _init_plain_text_state()
    current_section = None
    current_cat = None

    for raw in (text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        upper = line.upper()
        if upper == "LONG_TERM":
            current_section, current_cat = "LONG_TERM", None
            continue
        if upper == "SHORT_TERM":
            current_section, current_cat = "SHORT_TERM", None
            continue
        if line.endswith(":"):
            cat_name = line[:-1].strip()
            if (
                current_section == "LONG_TERM"
                and cat_name in state["LONG_TERM"]
            ):
                current_cat = cat_name
            elif (
                current_section == "SHORT_TERM"
                and cat_name in state["SHORT_TERM"]
            ):
                current_cat = cat_name
            else:
                current_cat = None
            continue

        if current_section and current_cat:
            state[current_section][current_cat].append(line)

    return state


def _render_plain_text_card(state: dict) -> str:
    """
    Render the section/category buckets back to plain text in template order.
    """
    lines: list[str] = []
    lines.append("LONG_TERM")
    for cat in LONG_TERM_CATEGORIES:
        lines.append(f"{cat}:")
        for entry in state["LONG_TERM"].get(cat, []):
            lines.append(entry)
    lines.append("")  # blank line between sections
    lines.append("SHORT_TERM")
    for cat in SHORT_TERM_CATEGORIES:
        lines.append(f"{cat}:")
        for entry in state["SHORT_TERM"].get(cat, []):
            lines.append(entry)
    return "\n".join(lines).strip()


def _parse_text_delta(delta_text: str) -> dict:
    """
    Parse TEXT_DELTA_SCRIBE output into section/category -> list of fact texts.
    """
    additions = {"LONG_TERM": {}, "SHORT_TERM": {}}
    current_section = None
    for raw in (delta_text or "").splitlines():
        line = raw.strip()
        if not line:
            continue
        upper = line.upper()
        if upper == "LONG_TERM":
            current_section = "LONG_TERM"
            continue
        if upper == "SHORT_TERM":
            current_section = "SHORT_TERM"
            continue
        if line.upper() == "EMPTY":
            continue

        if "::" not in line or not current_section:
            continue

        fact_key, fact_text = line.split("::", 1)
        fact_key = fact_key.strip()
        fact_text = fact_text.strip()
        if not fact_text:
            continue

        # Fact key format: LT:/path... or ST:/path...
        path = fact_key.split(":", 1)[-1]
        segments = [seg for seg in path.split("/") if seg]
        if not segments:
            continue
        top_level = segments[0]

        if current_section == "LONG_TERM":
            valid = LONG_TERM_CATEGORIES
        else:
            valid = SHORT_TERM_CATEGORIES
        if top_level not in valid:
            continue

        additions[current_section].setdefault(top_level, []).append(fact_text)

    return additions


def _merge_text_delta_into_card(current_text: str, delta_text: str) -> str:
    """
    Merge parsed text delta into the plain-text card and return updated text.
    """
    state = _parse_plain_text_card(current_text)
    additions = _parse_text_delta(delta_text)

    for section, cats in additions.items():
        for cat, lines in cats.items():
            existing = state[section].get(cat, [])
            norm_existing = {
                " ".join(e.strip().lower().split()) for e in existing if e.strip()
            }
            for line in lines:
                if not line:
                    continue
                norm_line = " ".join(line.strip().lower().split())
                if norm_line in norm_existing:
                    continue
                existing.append(line)
                norm_existing.add(norm_line)
            state[section][cat] = existing

    return _render_plain_text_card(state)


def build_conversation_slice(messages: list[dict]) -> str:
    lines: list[str] = []
    for msg in messages:
        role = "F" if msg.get("sender") == "fan" else "C"
        lines.append(f"[{role} turn {msg.get('turn_index')}] {msg.get('message_text', '').strip()}")
    return "\n".join(lines)


def safe_load_card(card_text: str) -> dict:
    """
    Parse a JSON string into a dict; accept dict input; log parse errors.
    """
    if not card_text:
        return {}
    if isinstance(card_text, dict):
        return card_text
    try:
        data = json.loads(card_text)
        if isinstance(data, dict):
            return data
    except Exception as exc:  # noqa: BLE001
        print(f"[fan_card_narrative_writer] Failed to parse model JSON: {exc}")
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
        .select("fan_identity_card,fan_identity_card_raw")
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
    current_plain_text = thread.get("fan_identity_card_raw")
    if not isinstance(current_plain_text, str) or not current_plain_text.strip():
        current_plain_text = PLAIN_TEXT_CARD_TEMPLATE
        try:
            SB.table("threads").update({"fan_identity_card_raw": current_plain_text}).eq(
                "id", thread_id
            ).execute()
        except Exception as exc:  # noqa: BLE001
            print("[fan_card_narrative_writer] Failed to seed fan_identity_card_raw:", exc, flush=True)
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
    _log_raw_delta(new_card_text, thread_id, fan_turn_index, "raw")

    delta_card = ensure_card_root(safe_load_card(new_card_text))
    if not delta_card["long_term"] and not delta_card["short_term"]:
        print(
            "[fan_card_narrative_writer] Model returned no JSON delta; skipping update.",
            flush=True,
        )
        _log_raw_delta(new_card_text, thread_id, fan_turn_index, "empty_delta")
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

    # Optional: generate a plain-text delta for append-only cards.
    try:
        text_user_prompt = build_text_scribe_prompt(
            merged_card, delta_card, current_plain_text
        )
        text_delta = runpod_call(TEXT_SCRIBE_SYSTEM_PROMPT, text_user_prompt)
        _log_text_delta(text_delta, thread_id, fan_turn_index)
        if text_delta and text_delta.strip().upper() != "EMPTY":
            updated_plain_text = _merge_text_delta_into_card(
                current_plain_text, text_delta
            )
            SB.table("threads").update({"fan_identity_card_raw": updated_plain_text}).eq(
                "id", thread_id
            ).execute()
    except Exception as exc:  # noqa: BLE001
        print("[fan_card_narrative_writer] Text scribe call failed:", exc, flush=True)

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
