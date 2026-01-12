import hashlib
import json
from typing import Any, Dict, Iterable, List, Optional


def _normalize_list(value) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(v) for v in value if v is not None and str(v).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        return [stripped] if stripped else []
    return [str(value)]


MEDIA_TYPE_ALIASES = {
    "image": "photo",
    "img": "photo",
    "photo": "photo",
    "photos": "photo",
    "picture": "photo",
    "pictures": "photo",
    "pic": "photo",
    "pics": "photo",
    "video": "video",
    "videos": "video",
    "audio": "voice",
    "voice": "voice",
    "voice_note": "voice",
    "voice_notes": "voice",
    "voicenote": "voice",
    "voicenotes": "voice",
    "sound": "voice",
    "sounds": "voice",
    "text": "text",
}


def _normalize_media_type(value: Optional[str]) -> Optional[str]:
    if not value:
        return None
    key = str(value).strip().lower()
    return MEDIA_TYPE_ALIASES.get(key, key)


def _normalize_media_types(values: Iterable[str]) -> List[str]:
    normalized: List[str] = []
    for val in values:
        mapped = _normalize_media_type(val)
        if mapped and mapped not in normalized:
            normalized.append(mapped)
    return normalized


def _pg_array_literal(values: Iterable[str]) -> str:
    escaped = []
    for val in values:
        v = str(val).replace('"', '\\"')
        escaped.append(f'"{v}"')
    return "{" + ",".join(escaped) + "}"


def _pg_in_list(values: Iterable[str]) -> str:
    escaped = []
    for val in values:
        v = str(val).replace('"', '\\"')
        escaped.append(f'"{v}"')
    return ",".join(escaped)


def _apply_array_filter(query, column: str, values: List[str], mode: str = "any"):
    if not values:
        return query
    mode_value = (mode or "any").lower()
    operator = "ov" if mode_value == "any" else "cs"
    if operator == "cs" and hasattr(query, "contains"):
        return query.contains(column, values)
    return query.filter(column, operator, _pg_array_literal(values))


def _apply_time_filter(query, time_values: List[str], mode: str | None = None):
    if not time_values:
        return query
    mode_value = (mode or "loose").lower()
    values = list(time_values)
    if mode_value != "strict":
        if "anytime" not in values:
            values.append("anytime")
        if hasattr(query, "or_"):
            in_list = _pg_in_list(values)
            return query.or_(f"time_of_day.in.({in_list}),time_of_day.is.null")
    return query.in_("time_of_day", values)


def _voice_excerpt(text: Optional[str], max_chars: int = 140) -> str:
    if not text:
        return ""
    cleaned = " ".join(text.split())
    if len(cleaned) <= max_chars:
        return cleaned
    return cleaned[: max_chars - 1].rstrip() + "…"


def _safe_int(value, default=None):
    try:
        return int(value)
    except Exception:
        return default


def _fetch_thread_used_content_ids(client, *, thread_id: int) -> set[int]:
    """
    Return content_ids already delivered in this thread (messages.content_id).
    """
    if not thread_id:
        return set()
    try:
        rows = (
            client.table("messages")
            .select("content_id")
            .eq("thread_id", thread_id)
            .execute()
            .data
            or []
        )
    except Exception:
        return set()

    used: set[int] = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        content_id = _safe_int(row.get("content_id"))
        if content_id is not None:
            used.add(content_id)
    for row in _fetch_thread_delivery_rows(client, thread_id=thread_id):
        if not isinstance(row, dict):
            continue
        content_id = _safe_int(row.get("content_id"))
        if content_id is not None:
            used.add(content_id)
    return used


def _fetch_thread_offer_rows(client, *, thread_id: int) -> List[Dict[str, Any]]:
    """
    Return content offers made in this thread (content_offers rows).

    Note: this table may not exist in all environments; fail open.
    """
    if not thread_id:
        return []
    try:
        return (
            client.table("content_offers")
            .select(
                "id,thread_id,message_id,content_id,offered_price,purchased,"
                "purchased_amount,purchased_at,created_at"
            )
            .eq("thread_id", thread_id)
            .order("created_at", desc=False)
            .execute()
            .data
            or []
        )
    except Exception:
        return []


def _fetch_thread_delivery_rows(client, *, thread_id: int) -> List[Dict[str, Any]]:
    """
    Return content deliveries made in this thread (content_deliveries rows).

    Note: this table may not exist in all environments; fail open.
    """
    if not thread_id:
        return []
    try:
        return (
            client.table("content_deliveries")
            .select("id,thread_id,message_id,content_id,created_at")
            .eq("thread_id", thread_id)
            .order("created_at", desc=False)
            .execute()
            .data
            or []
        )
    except Exception:
        return []

def _thread_excluded_content_ids(
    client, *, thread_id: int
) -> tuple[set[int], set[int], List[Dict[str, Any]]]:
    """
    Return (excluded_content_ids, used_content_ids, offer_rows) for a thread.

    Excluded = delivered (messages.content_id) ∪ offered (content_offers.content_id).
    """
    used_ids = _fetch_thread_used_content_ids(client, thread_id=thread_id)
    offer_rows = _fetch_thread_offer_rows(client, thread_id=thread_id)
    offered_ids: set[int] = set()
    for row in offer_rows:
        if not isinstance(row, dict):
            continue
        content_id = _safe_int(row.get("content_id"))
        if content_id is not None:
            offered_ids.add(content_id)
    return (used_ids | offered_ids), used_ids, offer_rows


def _sort_items(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Stable ordering:
    - sequence_position (ascending, missing last)
    - created_at (ascending, missing last)
    - id (ascending)
    """

    def sort_key(row: Dict[str, Any]):
        seq = row.get("sequence_position")
        seq_key = seq if isinstance(seq, int) else float("inf")
        created = row.get("created_at") or "9999-12-31T23:59:59Z"
        item_id = row.get("id") or 0
        return (seq_key, created, item_id)

    return sorted(rows, key=sort_key)


def _fetch_rows(
    client,
    *,
    creator_id: int,
    script_id: str | None = None,
    script_id_is_null: bool = False,
    shoot_id: str | None = None,
    include_long: bool = False,
    time_of_day: List[str] | None = None,
    time_mode: str | None = None,
    location_primary: str | None = None,
    outfit_category: str | None = None,
    outfit_layers: List[str] | None = None,
    outfit_layers_mode: str | None = None,
    body_focus: List[str] | None = None,
    body_focus_mode: str | None = None,
    mood_tags: List[str] | None = None,
    mood_tags_mode: str | None = None,
    action_tags: List[str] | None = None,
    action_tags_mode: str | None = None,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    select_fields = [
        "id",
        "creator_id",
        "media_type",
        "explicitness",
        "desc_short",
        "duration_seconds",
        "time_of_day",
        "location_primary",
        "outfit_category",
        "outfit_layers",
        "location_tags",
        "mood_tags",
        "action_tags",
        "body_focus",
        "camera_angle",
        "shot_type",
        "lighting",
        "script_id",
        "shoot_id",
        "stage",
        "sequence_position",
        "created_at",
    ]
    if include_long:
        select_fields.extend(["desc_long", "voice_transcript"])
    base_query = (
        client.table("content_items")
        .select(",".join(select_fields))
        .eq("creator_id", creator_id)
    )
    if script_id is not None:
        base_query = base_query.eq("script_id", script_id)
    if script_id_is_null:
        base_query = base_query.filter("script_id", "is", "null")
    if shoot_id is not None:
        base_query = base_query.eq("shoot_id", shoot_id)
    if time_of_day:
        base_query = _apply_time_filter(base_query, time_of_day, mode=time_mode)
    if location_primary:
        base_query = base_query.eq("location_primary", location_primary)
    if outfit_category:
        base_query = base_query.eq("outfit_category", outfit_category)
    if outfit_layers:
        mode = outfit_layers_mode or "any"
        base_query = _apply_array_filter(
            base_query, "outfit_layers", outfit_layers, mode=mode
        )
    if body_focus:
        mode = body_focus_mode or "any"
        base_query = _apply_array_filter(base_query, "body_focus", body_focus, mode=mode)
    if mood_tags:
        mode = mood_tags_mode or "any"
        base_query = _apply_array_filter(base_query, "mood_tags", mood_tags, mode=mode)
    if action_tags:
        mode = action_tags_mode or "any"
        base_query = _apply_array_filter(base_query, "action_tags", action_tags, mode=mode)
    if limit:
        base_query = base_query.limit(int(limit))

    rows = base_query.execute().data or []
    return _sort_items(rows)


def _fetch_scripts(client, *, creator_id: int, script_id: str | None = None) -> List[Dict[str, Any]]:
    query = (
        client.table("content_scripts")
        .select(
            "id,creator_id,shoot_id,title,summary,time_of_day,location_primary,"
            "outfit_category,focus_tags,created_at,meta"
        )
        .eq("creator_id", creator_id)
    )
    if script_id is not None:
        query = query.eq("id", script_id)
    # Keep a stable order: newest scripts last is fine; Hermes can decide.
    return query.execute().data or []


def _count_by_media_and_explicitness(rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, int]]:
    counts: Dict[str, Dict[str, int]] = {}
    for row in rows:
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        explicitness = (row.get("explicitness") or "unknown").strip() or "unknown"
        bucket = counts.setdefault(media_type, {})
        bucket[explicitness] = bucket.get(explicitness, 0) + 1
        bucket["total"] = bucket.get("total", 0) + 1
    return counts


def _make_pack_ref(value: str | None) -> str | None:
    if not value:
        return None
    seed = str(value).strip()
    if not seed:
        return None
    digest = hashlib.blake2s(seed.encode("utf-8"), digest_size=5).hexdigest()
    return f"pack_{digest}"


def _format_inventory(counts: Any) -> str:
    """
    Render nested counts like:
      "photo: 4 (sfw 2, tease 1, nsfw 1); video: 2 (nsfw 2); voice: 1 (tease 1)"
    """
    if not isinstance(counts, dict) or not counts:
        return "none"

    media_order = ["photo", "video", "voice", "text"]
    tier_order = ["sfw", "tease", "nsfw", "unknown"]

    def media_sort_key(media_type: str) -> tuple[int, str]:
        try:
            return (media_order.index(media_type), "")
        except ValueError:
            return (len(media_order), media_type)

    def tier_sort_key(tier: str) -> tuple[int, str]:
        try:
            return (tier_order.index(tier), "")
        except ValueError:
            return (len(tier_order), tier)

    parts: List[str] = []
    for media_type in sorted((str(k) for k in counts.keys()), key=media_sort_key):
        bucket = counts.get(media_type)
        if not isinstance(bucket, dict) or not bucket:
            continue

        total = bucket.get("total")
        if not isinstance(total, int):
            computed_total = 0
            for key, value in bucket.items():
                if key == "total":
                    continue
                if isinstance(value, int) and value > 0:
                    computed_total += value
            total = computed_total
        if not isinstance(total, int) or total <= 0:
            continue

        tiers_present = [
            str(k)
            for k, v in bucket.items()
            if k != "total" and isinstance(v, int) and v > 0
        ]
        tier_bits: List[str] = []
        for tier in sorted(tiers_present, key=tier_sort_key):
            value = bucket.get(tier)
            if isinstance(value, int) and value > 0:
                tier_bits.append(f"{tier} {value}")

        if tier_bits:
            parts.append(f"{media_type}: {total} ({', '.join(tier_bits)})")
        else:
            parts.append(f"{media_type}: {total}")

    return "; ".join(parts) if parts else "none"


def _normalize_tag(value: Optional[str]) -> str:
    if not value:
        return ""
    cleaned = str(value).strip().lower().replace("_", " ")
    cleaned = " ".join(cleaned.split())
    return cleaned


def _ammo_primary_tag_for_row(row: Dict[str, Any]) -> str:
    for key in (
        "action_tags",
        "location_tags",
        "shot_type",
        "camera_angle",
        "location_primary",
        "outfit_category",
        "mood_tags",
        "body_focus",
    ):
        values = _normalize_list(row.get(key))
        if values:
            return values[0]
    return ""


def _summarize_ammo_breakdown(
    rows: List[Dict[str, Any]], max_tags: int = 8
) -> Dict[str, str]:
    counts_by_media: Dict[str, Dict[str, int]] = {}
    for row in rows:
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        if media_type == "unknown":
            continue
        tag = _ammo_primary_tag_for_row(row) or "misc"
        bucket = counts_by_media.setdefault(media_type, {})
        normalized = _normalize_tag(tag)
        if not normalized:
            continue
        bucket[normalized] = bucket.get(normalized, 0) + 1

    summary: Dict[str, str] = {}
    for media_type, bucket in counts_by_media.items():
        if not bucket:
            continue
        items = sorted(bucket.items(), key=lambda item: (-item[1], item[0]))
        items = items[: max_tags or len(items)]
        summary[media_type] = ", ".join(f"{tag} {count}" for tag, count in items)
    return summary


def _select_global_ammo_rows(
    rows: List[Dict[str, Any]], scripts: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    script_shoot_ids = {
        str(s.get("shoot_id"))
        for s in scripts
        if s.get("shoot_id") is not None and str(s.get("shoot_id")).strip()
    }
    global_rows: List[Dict[str, Any]] = []
    for row in rows:
        if row.get("script_id"):
            continue
        shoot_id = row.get("shoot_id")
        if shoot_id is not None and str(shoot_id) in script_shoot_ids:
            continue
        global_rows.append(row)
    return global_rows


def _extract_day_theme(title: Optional[str]) -> str:
    if not title:
        return ""
    text = str(title).strip()
    if not text:
        return ""
    for sep in ("→", "->"):
        if sep in text:
            return text.split(sep, 1)[0].strip()
    return ""


def _auto_ammo_summary(
    *,
    title: Optional[str],
    ammo_inventory: str,
    core_scene_time_of_day: Optional[str],
) -> str:
    theme = _extract_day_theme(title)
    anchor = core_scene_time_of_day or "later"
    if theme:
        return (
            f"All-day arc: {theme}. Use the ammo as natural check-ins to build continuity "
            f"before the {anchor} core scene offer. Inventory: {ammo_inventory}."
        )
    return (
        f"All-day arc: casual check-ins that build continuity before the {anchor} core scene offer. "
        f"Inventory: {ammo_inventory}."
    )


def _build_script_index(
    scripts: List[Dict[str, Any]],
    items: List[Dict[str, Any]],
    *,
    include_focus_tags: bool = True,
) -> List[Dict[str, Any]]:
    items_by_script: Dict[str, List[Dict[str, Any]]] = {}
    items_by_shoot_extras: Dict[str, List[Dict[str, Any]]] = {}

    for row in items:
        script_id = row.get("script_id")
        shoot_id = row.get("shoot_id")
        if script_id:
            items_by_script.setdefault(script_id, []).append(row)
        elif shoot_id:
            items_by_shoot_extras.setdefault(shoot_id, []).append(row)

    index: List[Dict[str, Any]] = []
    for script in scripts:
        sid = script.get("id")
        shoot_id = script.get("shoot_id")
        script_items = items_by_script.get(sid, []) if sid else []
        extras = items_by_shoot_extras.get(shoot_id, []) if shoot_id else []
        entry = {
            "script_id": sid,
            "shoot_id": shoot_id,
            "title": script.get("title"),
            "summary": script.get("summary"),
            "time_of_day": script.get("time_of_day"),
            "location_primary": script.get("location_primary"),
            "outfit_category": script.get("outfit_category"),
            "created_at": script.get("created_at"),
            "counts": _count_by_media_and_explicitness(script_items),
            "extras_counts": _count_by_media_and_explicitness(extras),
        }
        if include_focus_tags:
            entry["focus_tags"] = script.get("focus_tags") or []
        index.append(entry)

    # Stable order: by created_at then script_id (so UI doesn't jump).
    def sort_key(row: Dict[str, Any]):
        return (row.get("created_at") or "", row.get("script_id") or "")

    return sorted(index, key=sort_key)


def _sort_newest_first(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Stable newest-first ordering:
    - created_at (descending, missing last)
    - id (descending)
    """

    def sort_key(row: Dict[str, Any]):
        created = row.get("created_at") or ""
        item_id = row.get("id") or 0
        return (created, item_id)

    return sorted(rows, key=sort_key, reverse=True)


def build_content_index(
    client,
    *,
    creator_id: int,
    thread_id: int | None = None,
    global_ammo_limit: int = 60,
    thread_activity_limit: int = 60,
) -> Dict[str, Any]:
    """
    Structured Hermes-only content context in 3 "bubbles":
      1) Scripts + per-script pack ammo (shoot extras)
      2) Global (scriptless) ammo, newest-first
      3) Thread content activity (sent + PPV offers)
    """

    ammo_limit = int(global_ammo_limit or 0)
    activity_limit = int(thread_activity_limit or 0)

    scripts = _fetch_scripts(client, creator_id=creator_id)
    excluded_ids: set[int] = set()
    offer_rows: List[Dict[str, Any]] = []
    if thread_id:
        excluded_ids, _used_ids, offer_rows = _thread_excluded_content_ids(
            client, thread_id=int(thread_id)
        )

    all_items_rows = _fetch_rows(client, creator_id=creator_id)
    available_items_rows = [
        row
        for row in all_items_rows
        if _safe_int(row.get("id")) not in excluded_ids
    ]

    meta_by_script_id: Dict[str, Dict[str, Any]] = {}
    for script in scripts:
        sid = script.get("id")
        if not sid:
            continue
        meta_val = script.get("meta")
        meta: Dict[str, Any] = {}
        if isinstance(meta_val, dict):
            meta = meta_val
        elif isinstance(meta_val, str) and meta_val.strip():
            try:
                parsed = json.loads(meta_val)
                if isinstance(parsed, dict):
                    meta = parsed
            except Exception:
                meta = {}
        meta_by_script_id[str(sid)] = meta

    script_index = _build_script_index(
        scripts, available_items_rows, include_focus_tags=True
    )
    for entry in script_index:
        sid = entry.get("script_id")
        shoot_id = entry.get("shoot_id")
        meta = meta_by_script_id.get(str(sid), {}) if sid else {}

        entry["ref"] = _make_pack_ref(sid) or _make_pack_ref(shoot_id)
        core_scene_time_of_day = entry.pop("time_of_day", None)
        entry["core_scene_time_of_day"] = core_scene_time_of_day

        script_summary = entry.pop("summary", None)
        entry["script_summary"] = (
            meta.get("script_summary")
            or meta.get("core_scene_summary")
            or meta.get("core_summary")
            or script_summary
            or ""
        )

        core_counts = entry.pop("counts", {})
        ammo_counts = entry.pop("extras_counts", {})
        entry["core_inventory"] = _format_inventory(core_counts)
        ammo_inventory = _format_inventory(ammo_counts)
        entry["ammo_inventory"] = ammo_inventory
        entry["ammo_summary"] = (
            meta.get("ammo_summary")
            or meta.get("ammo_blurb")
            or _auto_ammo_summary(
                title=entry.get("title"),
                ammo_inventory=ammo_inventory,
                core_scene_time_of_day=core_scene_time_of_day,
            )
        )

    global_ammo_rows = _select_global_ammo_rows(available_items_rows, scripts)
    global_ammo_rows = _sort_newest_first(global_ammo_rows)
    if ammo_limit:
        global_ammo_rows = global_ammo_rows[:ammo_limit]

    global_ammo_inventory = _format_inventory(
        _count_by_media_and_explicitness(global_ammo_rows)
    )

    sent_events: List[Dict[str, Any]] = []
    if thread_id and activity_limit:
        try:
            sent_rows = (
                client.table("messages")
                .select("id,turn_index,created_at,content_id")
                .eq("thread_id", int(thread_id))
                .order("turn_index", desc=True)
                .limit(activity_limit * 5)
                .execute()
                .data
                or []
            )
        except Exception:
            sent_rows = []
        message_ids = [
            row.get("id") for row in sent_rows if isinstance(row, dict) and row.get("id")
        ]
        message_id_set = {mid for mid in message_ids if mid is not None}
        deliveries_by_message_id: Dict[int, List[int]] = {}
        for row in _fetch_thread_delivery_rows(client, thread_id=int(thread_id)):
            msg_id = row.get("message_id")
            if msg_id is None or msg_id not in message_id_set:
                continue
            content_id = _safe_int(row.get("content_id"))
            if content_id is None:
                continue
            deliveries_by_message_id.setdefault(msg_id, []).append(content_id)
        for row in sent_rows:
            if not isinstance(row, dict):
                continue
            msg_id = row.get("id")
            if msg_id is None:
                continue
            content_ids = deliveries_by_message_id.get(msg_id, [])
            content_id = _safe_int(row.get("content_id"))
            if content_id is not None:
                content_ids.append(content_id)
            seen: set[int] = set()
            for cid in content_ids:
                if cid in seen:
                    continue
                seen.add(cid)
                sent_events.append(
                    {
                        "message_id": msg_id,
                        "turn_index": row.get("turn_index"),
                        "created_at": row.get("created_at"),
                        "content_id": cid,
                    }
                )
                if len(sent_events) >= activity_limit:
                    break
            if len(sent_events) >= activity_limit:
                break

    offer_events: List[Dict[str, Any]] = []
    if activity_limit:
        for row in _sort_newest_first(offer_rows)[:activity_limit]:
            if not isinstance(row, dict):
                continue
            content_id = _safe_int(row.get("content_id"))
            if content_id is None:
                continue
            purchased = row.get("purchased")
            offer_events.append(
                {
                    "message_id": row.get("message_id"),
                    "created_at": row.get("created_at"),
                    "content_id": content_id,
                    "status": "bought" if purchased is True else "pending",
                    "offered_price": row.get("offered_price"),
                }
            )

    return {
        "type": "content_index",
        "zoom": 1,
        "creator_id": creator_id,
        "thread_id": thread_id,
        "bubble_1": {
            "header": "BUBBLE 1: Scripts + Pack Ammo",
            "scripts": script_index,
        },
        "bubble_2": {
            "header": "BUBBLE 2: Global Ammo (scriptless, newest first)",
            "inventory": global_ammo_inventory,
            "items": _build_item_pack(global_ammo_rows, expand_media=[]),
        },
        "bubble_3": {
            "header": "BUBBLE 3: Thread Content (sent + offers)",
            "sent": sent_events,
            "offers": offer_events,
        },
    }


def build_content_pack(
    client,
    *,
    creator_id: int,
    thread_id: int | None = None,
    zoom: int | str = 0,
    script_id: str | None = None,
    include_shoot_extras: bool = True,
    media_expand: List[str] | str | None = None,
    limit: int | None = None,
) -> Dict[str, Any]:
    """
    Script-first content pack for Napoleon.
    Zoom 0: list all scripts (summaries + counts)
    Zoom 1: script header + all script items (compact) + same-shoot extras (compact)
    Zoom 2: expand requested media types (others stay compact)
    """

    zoom_level = _safe_int(zoom, 0) or 0
    expand_media = _normalize_media_types(_normalize_list(media_expand))
    effective_expand_media = expand_media if zoom_level >= 2 else []
    excluded_ids: set[int] = set()
    if thread_id:
        excluded_ids, _used_ids, _offer_rows = _thread_excluded_content_ids(
            client, thread_id=int(thread_id)
        )
    scripts = _fetch_scripts(client, creator_id=creator_id)
    # For Zoom 0 we return script list only (no items).
    if zoom_level <= 0 or not script_id:
        # We still need item rows to compute counts; fetch once.
        items_rows = _fetch_rows(client, creator_id=creator_id)
        if excluded_ids:
            items_rows = [
                row
                for row in items_rows
                if _safe_int(row.get("id")) not in excluded_ids
            ]
        meta_by_script_id: Dict[str, Dict[str, Any]] = {}
        for script in scripts:
            sid = script.get("id")
            if not sid:
                continue
            meta_val = script.get("meta")
            meta: Dict[str, Any] = {}
            if isinstance(meta_val, dict):
                meta = meta_val
            elif isinstance(meta_val, str) and meta_val.strip():
                try:
                    parsed = json.loads(meta_val)
                    if isinstance(parsed, dict):
                        meta = parsed
                except Exception:
                    meta = {}
            meta_by_script_id[str(sid)] = meta
        scripts_index = _build_script_index(
            scripts, items_rows, include_focus_tags=False
        )
        for entry in scripts_index:
            script_id_value = entry.get("script_id")
            shoot_id_value = entry.get("shoot_id")
            meta = meta_by_script_id.get(str(script_id_value), {}) if script_id_value else {}

            entry["ref"] = _make_pack_ref(script_id_value) or _make_pack_ref(shoot_id_value)
            entry.pop("script_id", None)
            entry.pop("shoot_id", None)
            core_scene_time_of_day = entry.pop("time_of_day", None)
            entry["core_scene_time_of_day"] = core_scene_time_of_day

            script_summary = entry.pop("summary", None)
            entry["script_summary"] = (
                meta.get("script_summary")
                or meta.get("core_scene_summary")
                or meta.get("core_summary")
                or script_summary
                or ""
            )

            core_counts = entry.pop("counts", {})
            ammo_counts = entry.pop("extras_counts", {})
            entry["core_inventory"] = _format_inventory(core_counts)
            ammo_inventory = _format_inventory(ammo_counts)
            entry["ammo_inventory"] = ammo_inventory

            entry["ammo_summary"] = (
                meta.get("ammo_summary")
                or meta.get("ammo_blurb")
                or _auto_ammo_summary(
                    title=entry.get("title"),
                    ammo_inventory=ammo_inventory,
                    core_scene_time_of_day=core_scene_time_of_day,
                )
            )
        global_ammo_rows = _select_global_ammo_rows(items_rows, scripts)
        global_ammo_inventory = _format_inventory(
            _count_by_media_and_explicitness(global_ammo_rows)
        )
        global_ammo_breakdown = _summarize_ammo_breakdown(global_ammo_rows)
        return {
            "zoom": 0,
            "scripts_header": "SCRIPTS + PACK AMMO",
            "scripts": scripts_index,
            "global_ammo": {
                "header": "GLOBAL AMMO (scriptless, compact)",
                "inventory": global_ammo_inventory,
                "breakdown": global_ammo_breakdown,
            },
        }

    # Fetch the chosen script header.
    chosen_script_rows = _fetch_scripts(client, creator_id=creator_id, script_id=script_id)
    script_header = chosen_script_rows[0] if chosen_script_rows else None

    script_items_rows = _fetch_rows(
        client,
        creator_id=creator_id,
        script_id=script_id,
        include_long=zoom_level >= 2,
        limit=limit,
    )
    if excluded_ids:
        script_items_rows = [
            row
            for row in script_items_rows
            if _safe_int(row.get("id")) not in excluded_ids
        ]
    shoot_extras_rows: List[Dict[str, Any]] = []
    shoot_id = None
    if script_header:
        shoot_id = script_header.get("shoot_id")
    if include_shoot_extras and shoot_id:
        shoot_extras_rows = _fetch_rows(
            client,
            creator_id=creator_id,
            shoot_id=shoot_id,
            script_id_is_null=True,
            include_long=zoom_level >= 2,
        )
        if excluded_ids:
            shoot_extras_rows = [
                row
                for row in shoot_extras_rows
                if _safe_int(row.get("id")) not in excluded_ids
            ]

    return {
        "zoom": zoom_level,
        "script": {
            "script_id": script_id,
            "shoot_id": shoot_id,
            "title": (script_header or {}).get("title") if script_header else None,
            "summary": (script_header or {}).get("summary") if script_header else None,
            "time_of_day": (script_header or {}).get("time_of_day") if script_header else None,
            "location_primary": (script_header or {}).get("location_primary") if script_header else None,
            "outfit_category": (script_header or {}).get("outfit_category") if script_header else None,
            "focus_tags": (script_header or {}).get("focus_tags") if script_header else [],
            "created_at": (script_header or {}).get("created_at") if script_header else None,
        },
        "filters": {
            "script_id": script_id,
            "media_expand": expand_media,
            "include_shoot_extras": bool(include_shoot_extras),
        },
        "script_items": _build_item_pack(
            script_items_rows, expand_media=effective_expand_media
        ),
        "shoot_extras": _build_item_pack(
            shoot_extras_rows, expand_media=effective_expand_media
        ),
    }


def _build_item_pack(rows: List[Dict[str, Any]], expand_media: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        detail = media_type in expand_media
        voice_excerpt = ""
        if media_type == "voice":
            voice_excerpt = _voice_excerpt(
                row.get("voice_transcript") or row.get("desc_short")
            )
        item = {
            "id": row.get("id"),
            "media_type": media_type,
            "explicitness": row.get("explicitness"),
            "duration_seconds": row.get("duration_seconds"),
            "desc_short": row.get("desc_short"),
            "desc_long": row.get("desc_long") if detail else None,
            "voice_transcript": row.get("voice_transcript") if detail else None,
            "voice_excerpt": voice_excerpt,
            "time_of_day": row.get("time_of_day"),
            "location_primary": row.get("location_primary"),
            "outfit_category": row.get("outfit_category"),
            "outfit_layers": row.get("outfit_layers"),
            "body_focus": row.get("body_focus"),
            "action_tags": row.get("action_tags"),
            "mood_tags": row.get("mood_tags"),
            "camera_angle": row.get("camera_angle"),
            "shot_type": row.get("shot_type"),
            "lighting": row.get("lighting"),
            "script_id": row.get("script_id"),
            "shoot_id": row.get("shoot_id"),
            "stage": row.get("stage"),
            "sequence_position": row.get("sequence_position"),
            "detail_level": "full" if detail else "compact",
        }
        items.append(item)

    return items


def format_content_pack(pack: Dict[str, Any]) -> str:
    if not pack:
        return ""
    return json.dumps(pack, ensure_ascii=False)
