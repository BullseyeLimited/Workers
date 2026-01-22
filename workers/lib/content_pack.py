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

    def expand(values: List[str]) -> List[str]:
        expanded: List[str] = []
        seen: set[str] = set()
        for raw in values:
            token = str(raw or "").strip().lower()
            if not token:
                continue
            if token in {"day", "daytime", "morning", "afternoon", "noon"}:
                options = ["day", "morning", "afternoon"]
            elif token in {"night", "nighttime", "evening"}:
                options = ["night", "evening"]
            elif token in {"anytime", "unspecified", "any"}:
                options = ["anytime"]
            else:
                options = [token]
            for opt in options:
                if opt in seen:
                    continue
                seen.add(opt)
                expanded.append(opt)
        return expanded

    mode_value = (mode or "loose").lower()
    values = expand(list(time_values))
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


def _parse_id_list(value) -> List[int]:
    if value is None:
        return []
    raw_items: List[Any]
    if isinstance(value, list):
        raw_items = value
    elif isinstance(value, str):
        raw_items = [v.strip() for v in value.replace(" ", ",").split(",") if v.strip()]
    else:
        raw_items = [value]

    ids: List[int] = []
    for raw in raw_items:
        cid = _safe_int(raw)
        if cid is None or cid in ids:
            continue
        ids.append(cid)
    return ids


def _fetch_items_by_ids(
    client,
    *,
    creator_id: int,
    ids: List[int],
    include_long: bool = False,
) -> List[Dict[str, Any]]:
    if not ids:
        return []
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
        "voice_transcript",
        "script_id",
        "shoot_id",
        "stage",
        "sequence_position",
        "created_at",
    ]
    if include_long:
        select_fields.extend(["desc_long"])
    query = (
        client.table("content_items")
        .select(",".join(select_fields))
        .eq("creator_id", creator_id)
        .in_("id", ids)
    )
    return query.execute().data or []


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
        "voice_transcript",
        "script_id",
        "shoot_id",
        "stage",
        "sequence_position",
        "created_at",
    ]
    if include_long:
        select_fields.extend(["desc_long"])
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
            "outfit_category,focus_tags,created_at,meta,script_summary,ammo_summary"
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


def _clean_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _ammo_primary_tag_for_row(row: Dict[str, Any]) -> str:
    for key in (
        "action_tags",
        "location_tags",
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


STAGE_ORDER = ["setup", "tease", "build", "climax", "after"]


def _stage_rank(value: Any) -> int:
    if not value:
        return len(STAGE_ORDER)
    cleaned = str(value).strip().lower()
    if not cleaned:
        return len(STAGE_ORDER)
    try:
        return STAGE_ORDER.index(cleaned)
    except ValueError:
        return len(STAGE_ORDER)


def _sort_core_items(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(row: Dict[str, Any]):
        stage_key = _stage_rank(row.get("stage"))
        seq = row.get("sequence_position")
        seq_key = seq if isinstance(seq, int) else float("inf")
        created = row.get("created_at") or ""
        item_id = row.get("id") or 0
        return (stage_key, seq_key, created, item_id)

    return sorted(rows, key=sort_key)




def _is_empty_field(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, dict):
        return not value
    if isinstance(value, list):
        return not value
    return False


def _strip_empty_fields(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned: Dict[str, Any] = {}
        for key, item in value.items():
            cleaned_item = _strip_empty_fields(item)
            if _is_empty_field(cleaned_item):
                continue
            cleaned[key] = cleaned_item
        return cleaned
    if isinstance(value, list):
        cleaned_list: List[Any] = []
        for item in value:
            cleaned_item = _strip_empty_fields(item)
            if _is_empty_field(cleaned_item):
                continue
            cleaned_list.append(cleaned_item)
        return cleaned_list
    return value


def _build_hermes_item(
    row: Dict[str, Any],
    *,
    include_context: bool,
    include_stage: bool,
    include_sequence: bool,
    offer_status: Optional[str] = None,
    offered_price: Any = None,
) -> str:
    return _build_item_line(
        row,
        include_context=include_context,
        include_stage=include_stage,
        include_sequence=include_sequence,
        offer_status=offer_status,
        offered_price=offered_price,
    )


def _join_item_values(values: Iterable[Any]) -> str:
    cleaned = [str(v).strip() for v in values if v is not None and str(v).strip()]
    return " + ".join(cleaned)


def _append_scalar(parts: List[str], label: str, value: Any) -> None:
    if value is None:
        return
    cleaned = str(value).strip()
    if not cleaned:
        return
    parts.append(f"{label}: {cleaned}")


def _append_group(parts: List[str], label: str, values: Iterable[Any]) -> None:
    joined = _join_item_values(values)
    if not joined:
        return
    parts.append(f"{label}: {joined}")


def _build_item_line(
    row: Dict[str, Any],
    *,
    include_context: bool,
    include_stage: bool,
    include_sequence: bool,
    include_detail: bool = False,
    offer_status: Optional[str] = None,
    offered_price: Any = None,
) -> str:
    media_type = _normalize_media_type(row.get("media_type")) or "unknown"
    media_label = {
        "voice": "voice note",
        "photo": "photo",
        "video": "video",
        "text": "text",
    }.get(media_type, media_type)

    parts: List[str] = []
    item_id = row.get("id")
    if item_id is not None:
        parts.append(f"id {item_id}")
    if media_label:
        parts.append(media_label)
    explicitness = row.get("explicitness")
    if explicitness:
        parts.append(str(explicitness))
    desc_short = row.get("desc_short")
    if desc_short:
        parts.append(str(desc_short))

    if media_type in {"video", "voice"}:
        duration = row.get("duration_seconds")
        if duration is not None:
            parts.append(f"duration: {duration} sec")

    if media_type == "voice":
        transcript = row.get("voice_transcript")
        if transcript:
            excerpt = _voice_excerpt(transcript)
            if excerpt and excerpt != desc_short:
                parts.append(f"audio: {excerpt}")
        if include_detail and transcript:
            parts.append(f"audio_full: {transcript}")

    if include_detail:
        desc_long = row.get("desc_long")
        if desc_long:
            parts.append(f"detail: {desc_long}")

    if include_context:
        _append_scalar(parts, "time", row.get("time_of_day"))
        _append_scalar(parts, "location", row.get("location_primary"))
        _append_scalar(parts, "outfit", row.get("outfit_category"))

    _append_group(parts, "fit", _normalize_list(row.get("outfit_layers")))
    _append_group(parts, "focus", _normalize_list(row.get("body_focus")))
    _append_group(parts, "action", _normalize_list(row.get("action_tags")))
    _append_group(parts, "mood", _normalize_list(row.get("mood_tags")))

    if include_stage:
        _append_scalar(parts, "stage", row.get("stage"))
    if include_sequence:
        sequence = row.get("sequence_position")
        if sequence is not None:
            _append_scalar(parts, "sequence", sequence)

    if offer_status:
        offer_bits = [offer_status]
        if offered_price is not None:
            offer_bits.append(str(offered_price))
        parts.append(f"offer: {' '.join(offer_bits)}")

    return ", ".join(parts)


def _build_napoleon_item_line(
    row: Dict[str, Any],
    *,
    include_context: bool,
    include_stage: bool,
    include_sequence: bool,
    include_detail: bool = False,
) -> str:
    return _build_item_line(
        row,
        include_context=include_context,
        include_stage=include_stage,
        include_sequence=include_sequence,
        include_detail=include_detail,
    )


def _build_hermes_script_header(
    script_row: Dict[str, Any],
    meta: Dict[str, Any],
    *,
    core_rows: List[Dict[str, Any]],
    ammo_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    sid = script_row.get("id")
    shoot_id = script_row.get("shoot_id")
    core_scene_time_of_day = script_row.get("time_of_day")
    script_summary_column = _clean_text(script_row.get("script_summary"))
    ammo_summary_column = _clean_text(script_row.get("ammo_summary"))

    script_summary = (
        script_summary_column
        or meta.get("script_summary")
        or meta.get("core_scene_summary")
        or meta.get("core_summary")
        or script_row.get("summary")
        or ""
    )

    core_inventory = _format_inventory(_count_by_media_and_explicitness(core_rows))
    ammo_inventory = _format_inventory(_count_by_media_and_explicitness(ammo_rows))
    ammo_summary = (
        ammo_summary_column
        or meta.get("ammo_summary")
        or meta.get("ammo_blurb")
        or _auto_ammo_summary(
            title=script_row.get("title"),
            ammo_inventory=ammo_inventory,
            core_scene_time_of_day=core_scene_time_of_day,
        )
    )

    header = {
        "script_id": sid,
        "ref": _make_pack_ref(sid) or _make_pack_ref(shoot_id),
        "title": script_row.get("title"),
        "created_at": script_row.get("created_at"),
        "core_scene_time_of_day": core_scene_time_of_day,
        "location_primary": script_row.get("location_primary"),
        "outfit_category": script_row.get("outfit_category"),
        "focus_tags": script_row.get("focus_tags") or [],
        "script_summary": script_summary,
        "ammo_summary": ammo_summary,
        "core_inventory": core_inventory,
        "ammo_inventory": ammo_inventory,
    }
    return _strip_empty_fields(header)


def _group_rows_by_media_type(
    rows: List[Dict[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        groups.setdefault(media_type, []).append(row)
    return groups


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
    script_by_id: Dict[str, Dict[str, Any]] = {}
    for script in scripts:
        sid = script.get("id")
        if not sid:
            continue
        script_by_id[str(sid)] = script
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

    items_by_script: Dict[str, List[Dict[str, Any]]] = {}
    items_by_shoot: Dict[str, List[Dict[str, Any]]] = {}
    for row in available_items_rows:
        script_id = row.get("script_id")
        if script_id:
            items_by_script.setdefault(str(script_id), []).append(row)
            continue
        shoot_id = row.get("shoot_id")
        if shoot_id:
            items_by_shoot.setdefault(str(shoot_id), []).append(row)

    script_blocks: List[Dict[str, Any]] = []
    scripts_sorted = sorted(
        scripts, key=lambda row: (row.get("created_at") or "", row.get("id") or "")
    )
    for script in scripts_sorted:
        sid = script.get("id")
        if not sid:
            continue
        sid_key = str(sid)
        core_rows = items_by_script.get(sid_key, [])
        shoot_id = script.get("shoot_id")
        ammo_rows = items_by_shoot.get(str(shoot_id), []) if shoot_id else []
        if not core_rows:
            # Do not expose script ammo until core items are finalized.
            continue
        core_ready = [
            row
            for row in core_rows
            if row.get("stage") and isinstance(row.get("sequence_position"), int)
        ]
        if len(core_ready) != len(core_rows):
            # Fail-safe: hide scripts until every core item has stage + sequence_position.
            continue
        core_rows = core_ready
        meta = meta_by_script_id.get(sid_key, {})
        header = _build_hermes_script_header(
            script, meta, core_rows=core_rows, ammo_rows=ammo_rows
        )
        ammo_rows_sorted = _sort_newest_first(ammo_rows)
        core_rows_sorted = _sort_core_items(core_rows)
        ammo_items = [
            _build_hermes_item(
                row,
                include_context=True,
                include_stage=False,
                include_sequence=False,
            )
            for row in ammo_rows_sorted
        ]
        core_items = [
            _build_hermes_item(
                row,
                include_context=False,
                include_stage=True,
                include_sequence=True,
            )
            for row in core_rows_sorted
        ]
        script_block: Dict[str, Any] = {}
        script_block.update(header)
        if ammo_items:
            script_block["ammo_items"] = ammo_items
        if core_items:
            script_block["core_items"] = core_items
        script_blocks.append(_strip_empty_fields(script_block))

    global_ammo_rows = _select_global_ammo_rows(available_items_rows, scripts)
    global_ammo_rows = _sort_newest_first(global_ammo_rows)
    if ammo_limit:
        global_ammo_rows = global_ammo_rows[:ammo_limit]

    global_ammo_inventory = _format_inventory(
        _count_by_media_and_explicitness(global_ammo_rows)
    )
    global_groups: List[Dict[str, Any]] = []
    grouped_global = _group_rows_by_media_type(global_ammo_rows)
    for media_type in ["photo", "video", "voice", "text", "unknown"]:
        rows = grouped_global.get(media_type, [])
        if not rows:
            continue
        rows_sorted = _sort_newest_first(rows)
        items = [
            _build_hermes_item(
                row,
                include_context=True,
                include_stage=False,
                include_sequence=False,
            )
            for row in rows_sorted
        ]
        if items:
            global_groups.append(
                {
                    "media_type": media_type,
                    "items": items,
                }
            )

    items_by_id: Dict[int, Dict[str, Any]] = {}
    for row in all_items_rows:
        content_id = _safe_int(row.get("id"))
        if content_id is None:
            continue
        items_by_id[content_id] = row

    activity_map: Dict[int, Dict[str, Any]] = {}
    if thread_id and activity_limit:
        try:
            sent_rows = (
                client.table("messages")
                .select("id,created_at,content_id")
                .eq("thread_id", int(thread_id))
                .order("created_at", desc=True)
                .limit(activity_limit * 6)
                .execute()
                .data
                or []
            )
        except Exception:
            sent_rows = []

        for row in sent_rows:
            if not isinstance(row, dict):
                continue
            content_id = _safe_int(row.get("content_id"))
            if content_id is None:
                continue
            created_at = row.get("created_at")
            entry = activity_map.setdefault(
                content_id,
                {
                    "content_id": content_id,
                    "sent_at": None,
                    "offer_at": None,
                    "offer_status": None,
                    "offered_price": None,
                },
            )
            if created_at and (not entry["sent_at"] or created_at > entry["sent_at"]):
                entry["sent_at"] = created_at

        for row in _fetch_thread_delivery_rows(client, thread_id=int(thread_id)):
            if not isinstance(row, dict):
                continue
            content_id = _safe_int(row.get("content_id"))
            if content_id is None:
                continue
            created_at = row.get("created_at")
            entry = activity_map.setdefault(
                content_id,
                {
                    "content_id": content_id,
                    "sent_at": None,
                    "offer_at": None,
                    "offer_status": None,
                    "offered_price": None,
                },
            )
            if created_at and (not entry["sent_at"] or created_at > entry["sent_at"]):
                entry["sent_at"] = created_at

        for row in offer_rows:
            if not isinstance(row, dict):
                continue
            content_id = _safe_int(row.get("content_id"))
            if content_id is None:
                continue
            created_at = row.get("created_at")
            status = "bought" if row.get("purchased") is True else "pending"
            entry = activity_map.setdefault(
                content_id,
                {
                    "content_id": content_id,
                    "sent_at": None,
                    "offer_at": None,
                    "offer_status": None,
                    "offered_price": None,
                },
            )
            if created_at and (not entry["offer_at"] or created_at > entry["offer_at"]):
                entry["offer_at"] = created_at
                entry["offer_status"] = status
                entry["offered_price"] = row.get("offered_price")

    activity_entries: List[Dict[str, Any]] = []
    for entry in activity_map.values():
        last_activity = max(entry.get("sent_at") or "", entry.get("offer_at") or "")
        entry["last_activity_at"] = last_activity
        activity_entries.append(entry)
    activity_entries = sorted(
        activity_entries,
        key=lambda item: item.get("last_activity_at") or "",
        reverse=True,
    )
    if activity_limit:
        activity_entries = activity_entries[:activity_limit]

    activity_by_id = {entry["content_id"]: entry for entry in activity_entries}

    script_id_by_shoot: Dict[str, str] = {}
    for script in scripts:
        shoot_id = script.get("shoot_id")
        sid = script.get("id")
        if shoot_id is None or not sid:
            continue
        script_id_by_shoot[str(shoot_id)] = str(sid)

    used_script_map: Dict[str, Dict[str, Any]] = {}
    used_scriptless: List[Dict[str, Any]] = []
    for entry in activity_entries:
        content_id = entry.get("content_id")
        if content_id is None:
            continue
        row = items_by_id.get(content_id)
        if not row:
            continue
        script_id = row.get("script_id")
        if script_id:
            sid_key = str(script_id)
            group = used_script_map.setdefault(
                sid_key,
                {
                    "script": script_by_id.get(sid_key),
                    "core_rows": [],
                    "ammo_rows": [],
                    "last_activity_at": "",
                },
            )
            group["core_rows"].append(row)
            group["last_activity_at"] = max(
                group["last_activity_at"], entry.get("last_activity_at") or ""
            )
            continue
        shoot_id = row.get("shoot_id")
        sid_key = script_id_by_shoot.get(str(shoot_id)) if shoot_id else None
        if sid_key:
            group = used_script_map.setdefault(
                sid_key,
                {
                    "script": script_by_id.get(sid_key),
                    "core_rows": [],
                    "ammo_rows": [],
                    "last_activity_at": "",
                },
            )
            group["ammo_rows"].append(row)
            group["last_activity_at"] = max(
                group["last_activity_at"], entry.get("last_activity_at") or ""
            )
        else:
            used_scriptless.append(row)

    used_script_blocks: List[Dict[str, Any]] = []
    for sid_key, group in sorted(
        used_script_map.items(),
        key=lambda item: item[1].get("last_activity_at") or "",
        reverse=True,
    ):
        script_row = group.get("script") or {}
        if not script_row:
            continue
        meta = meta_by_script_id.get(str(sid_key), {})
        core_rows = group.get("core_rows") or []
        ammo_rows = group.get("ammo_rows") or []
        header = _build_hermes_script_header(
            script_row, meta, core_rows=core_rows, ammo_rows=ammo_rows
        )
        script_block: Dict[str, Any] = {}
        script_block.update(header)
        if ammo_rows:
            ammo_rows_sorted = _sort_newest_first(ammo_rows)
            ammo_items: List[Dict[str, Any]] = []
            for row in ammo_rows_sorted:
                activity = activity_by_id.get(_safe_int(row.get("id")))
                item = _build_hermes_item(
                    row,
                    include_context=True,
                    include_stage=False,
                    include_sequence=False,
                    offer_status=activity.get("offer_status") if activity else None,
                    offered_price=activity.get("offered_price") if activity else None,
                )
                ammo_items.append(item)
            if ammo_items:
                script_block["ammo_items"] = ammo_items
        if core_rows:
            core_rows_sorted = _sort_core_items(core_rows)
            core_items: List[Dict[str, Any]] = []
            for row in core_rows_sorted:
                activity = activity_by_id.get(_safe_int(row.get("id")))
                item = _build_hermes_item(
                    row,
                    include_context=False,
                    include_stage=True,
                    include_sequence=True,
                    offer_status=activity.get("offer_status") if activity else None,
                    offered_price=activity.get("offered_price") if activity else None,
                )
                core_items.append(item)
            if core_items:
                script_block["core_items"] = core_items
        used_script_blocks.append(_strip_empty_fields(script_block))

    used_scriptless_groups: List[Dict[str, Any]] = []
    if used_scriptless:
        grouped_used = _group_rows_by_media_type(used_scriptless)
        for media_type in ["photo", "video", "voice", "text", "unknown"]:
            rows = grouped_used.get(media_type, [])
            if not rows:
                continue
            rows_sorted = sorted(
                rows,
                key=lambda row: (activity_by_id.get(_safe_int(row.get("id")), {}).get("last_activity_at") or ""),
                reverse=True,
            )
            items: List[Dict[str, Any]] = []
            for row in rows_sorted:
                activity = activity_by_id.get(_safe_int(row.get("id")))
                item = _build_hermes_item(
                    row,
                    include_context=True,
                    include_stage=False,
                    include_sequence=False,
                    offer_status=activity.get("offer_status") if activity else None,
                    offered_price=activity.get("offered_price") if activity else None,
                )
                items.append(item)
            if items:
                used_scriptless_groups.append(
                    {
                        "media_type": media_type,
                        "items": items,
                    }
                )

    return {
        "type": "content_index",
        "zoom": 1,
        "creator_id": creator_id,
        "thread_id": thread_id,
        "bubble_1": {
            "header": "BUBBLE 1: Scripts + Pack Ammo",
            "scripts": script_blocks,
        },
        "bubble_2": {
            "header": "BUBBLE 2: Global Ammo (scriptless, newest first)",
            "inventory": global_ammo_inventory,
            "groups": global_groups,
        },
        "bubble_3": {
            "header": "BUBBLE 3: Thread Content (used + offers)",
            "scripts": used_script_blocks,
            "scriptless": used_scriptless_groups,
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
    content_ids: List[int] | str | None = None,
    global_focus_ids: List[int] | str | None = None,
    global_focus_limit: int | None = None,
    limit: int | None = None,
) -> Dict[str, Any]:
    """
    Script-first content pack for Napoleon.
    Zoom 0: list all scripts (summaries + counts)
    Zoom 1: script header + all script items (compact) + same-shoot extras (compact)
    Zoom 2: expanded detail lines for specific content ids or media types
    """

    zoom_level = _safe_int(zoom, 0) or 0
    expand_media = _normalize_media_types(_normalize_list(media_expand))
    effective_expand_media = expand_media if zoom_level >= 2 else []
    content_id_list: List[int] = []
    if content_ids is not None:
        for raw in _normalize_list(content_ids):
            cid = _safe_int(raw)
            if cid is not None:
                content_id_list.append(cid)
    global_focus_list = _parse_id_list(global_focus_ids)
    focus_limit = int(global_focus_limit) if global_focus_limit is not None else 5
    if focus_limit and global_focus_list:
        global_focus_list = global_focus_list[:focus_limit]
    excluded_ids: set[int] = set()
    if thread_id:
        excluded_ids, _used_ids, _offer_rows = _thread_excluded_content_ids(
            client, thread_id=int(thread_id)
        )
    scripts = _fetch_scripts(client, creator_id=creator_id)
    script_shoot_ids = {
        str(s.get("shoot_id"))
        for s in scripts
        if s.get("shoot_id") is not None and str(s.get("shoot_id")).strip()
    }

    global_focus_rows: List[Dict[str, Any]] = []
    if global_focus_list:
        fetched_rows = _fetch_items_by_ids(
            client,
            creator_id=creator_id,
            ids=global_focus_list,
            include_long=zoom_level >= 2,
        )
        rows_by_id = {}
        for row in fetched_rows:
            cid = _safe_int(row.get("id"))
            if cid is not None:
                rows_by_id[cid] = row
        for cid in global_focus_list:
            row = rows_by_id.get(cid)
            if not row:
                continue
            if cid in excluded_ids:
                continue
            if row.get("script_id"):
                continue
            shoot_id = row.get("shoot_id")
            if shoot_id is not None and str(shoot_id) in script_shoot_ids:
                continue
            global_focus_rows.append(row)

    def _wants_detail(row: Dict[str, Any], *, expand_all: bool, detail_types: set[str]) -> bool:
        if expand_all:
            return True
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        return media_type in detail_types

    def _render_global_focus_items(
        *,
        include_detail: bool,
        expand_all: bool = False,
        detail_types: set[str] | None = None,
    ) -> List[str]:
        if not global_focus_rows:
            return []
        if not include_detail:
            return [
                _build_napoleon_item_line(
                    row,
                    include_context=True,
                    include_stage=False,
                    include_sequence=False,
                )
                for row in global_focus_rows
            ]
        detail_types = detail_types or set()
        return [
            _build_napoleon_item_line(
                row,
                include_context=True,
                include_stage=False,
                include_sequence=False,
                include_detail=_wants_detail(row, expand_all=expand_all, detail_types=detail_types),
            )
            for row in global_focus_rows
        ]
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
        script_by_id: Dict[str, Dict[str, Any]] = {}
        for script in scripts:
            sid = script.get("id")
            if not sid:
                continue
            script_by_id[str(sid)] = script
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
        scripts_index = _build_script_index(scripts, items_rows, include_focus_tags=False)
        scripts_index = sorted(
            scripts_index,
            key=lambda row: (row.get("created_at") or "", row.get("script_id") or ""),
            reverse=True,
        )
        for entry in scripts_index:
            script_id_value = entry.get("script_id")
            shoot_id_value = entry.get("shoot_id")
            meta = meta_by_script_id.get(str(script_id_value), {}) if script_id_value else {}
            script_row = script_by_id.get(str(script_id_value), {}) if script_id_value else {}
            script_summary_column = _clean_text(script_row.get("script_summary"))
            ammo_summary_column = _clean_text(script_row.get("ammo_summary"))

            entry.pop("script_id", None)
            entry.pop("shoot_id", None)
            entry.pop("created_at", None)
            core_scene_time_of_day = entry.pop("time_of_day", None)
            entry["core_scene_time_of_day"] = core_scene_time_of_day

            script_summary = entry.pop("summary", None)
            entry["script_summary"] = (
                script_summary_column
                or meta.get("script_summary")
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
                ammo_summary_column
                or meta.get("ammo_summary")
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
        pack = {
            "zoom": 0,
            "scripts_header": "SCRIPTS + PACK AMMO",
            "scripts": scripts_index,
            "global_ammo": {
                "header": "GLOBAL AMMO (scriptless, compact)",
                "inventory": global_ammo_inventory,
                "breakdown": global_ammo_breakdown,
            },
        }
        global_focus_items = _render_global_focus_items(include_detail=False)
        if global_focus_items:
            pack["global_focus"] = {
                "header": "GLOBAL AMMO FOCUS",
                "items": global_focus_items,
            }
        return pack

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

    script_payload = _strip_empty_fields(
        {
            "title": (script_header or {}).get("title") if script_header else None,
            "summary": (script_header or {}).get("summary") if script_header else None,
            "time_of_day": (script_header or {}).get("time_of_day") if script_header else None,
            "location_primary": (script_header or {}).get("location_primary") if script_header else None,
            "outfit_category": (script_header or {}).get("outfit_category") if script_header else None,
            "focus_tags": (script_header or {}).get("focus_tags") if script_header else [],
        }
    )
    if content_id_list:
        wanted = {cid for cid in content_id_list if cid is not None}
        by_id: Dict[int, Dict[str, Any]] = {}
        for row in script_items_rows + shoot_extras_rows:
            cid = _safe_int(row.get("id"))
            if cid is None:
                continue
            by_id[cid] = row
        selected_rows = [by_id[cid] for cid in content_id_list if cid in by_id]
        script_items_rows = [row for row in selected_rows if row.get("script_id")]
        shoot_extras_rows = [row for row in selected_rows if not row.get("script_id")]

    script_items_rows = _sort_core_items(script_items_rows)
    shoot_extras_rows = _sort_newest_first(shoot_extras_rows)

    if zoom_level == 1:
        pack = {
            "zoom": zoom_level,
            "script": script_payload,
            "script_items": [
                _build_napoleon_item_line(
                    row,
                    include_context=False,
                    include_stage=True,
                    include_sequence=True,
                )
                for row in script_items_rows
            ],
            "shoot_extras": [
                _build_napoleon_item_line(
                    row,
                    include_context=True,
                    include_stage=False,
                    include_sequence=False,
                )
                for row in shoot_extras_rows
            ],
        }
        global_focus_items = _render_global_focus_items(include_detail=False)
        if global_focus_items:
            pack["global_focus"] = {
                "header": "GLOBAL AMMO FOCUS",
                "items": global_focus_items,
            }
        return pack

    if zoom_level >= 2:
        detail_types = set(effective_expand_media)
        expand_all = not detail_types or bool(content_id_list)

        items_rows = script_items_rows + shoot_extras_rows
        pack = {
            "zoom": zoom_level,
            "items": [
                _build_napoleon_item_line(
                    row,
                    include_context=True,
                    include_stage=True,
                    include_sequence=True,
                    include_detail=_wants_detail(row, expand_all=expand_all, detail_types=detail_types),
                )
                for row in items_rows
            ],
        }
        global_focus_items = _render_global_focus_items(
            include_detail=True,
            expand_all=expand_all,
            detail_types=detail_types,
        )
        if global_focus_items:
            pack["global_focus"] = {
                "header": "GLOBAL AMMO FOCUS",
                "items": global_focus_items,
            }
        return pack

    return {
        "zoom": zoom_level,
        "script": script_payload,
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
        item: Dict[str, Any] = {
            "id": row.get("id"),
            "media_type": media_type,
            "explicitness": row.get("explicitness"),
            "desc_short": row.get("desc_short"),
            "time_of_day": row.get("time_of_day"),
            "location_primary": row.get("location_primary"),
            "outfit_category": row.get("outfit_category"),
            "outfit_layers": row.get("outfit_layers"),
            "body_focus": row.get("body_focus"),
            "action_tags": row.get("action_tags"),
            "mood_tags": row.get("mood_tags"),
            "camera_angle": row.get("camera_angle"),
            "stage": row.get("stage"),
            "sequence_position": row.get("sequence_position"),
            "detail_level": "full" if detail else "compact",
        }
        if media_type in {"video", "voice"}:
            item["duration_seconds"] = row.get("duration_seconds")
        if detail:
            item["desc_long"] = row.get("desc_long")
        if media_type == "voice":
            if detail:
                item["voice_transcript"] = row.get("voice_transcript")
            voice_excerpt = _voice_excerpt(
                row.get("voice_transcript") or row.get("desc_short")
            )
            if voice_excerpt:
                item["voice_excerpt"] = voice_excerpt
        items.append(_strip_empty_fields(item))

    return items


def format_content_pack(pack: Dict[str, Any]) -> str:
    if not pack:
        return ""
    return json.dumps(pack, ensure_ascii=False)


def format_content_pack_for_napoleon(pack: Dict[str, Any]) -> str:
    """
    Human/LLM-friendly content pack formatting.
    This is intentionally *not* JSON to keep Napoleon prompts easy to scan.
    """
    if not pack or not isinstance(pack, dict):
        return ""

    zoom_level = _safe_int(pack.get("zoom"), 0) or 0
    lines: List[str] = [f"CONTENT PACK (ZOOM {zoom_level})"]

    def add_header(text: str) -> None:
        lines.append("")
        lines.append(str(text).strip())

    def add_kv(label: str, value: Any) -> None:
        if value is None:
            return
        if isinstance(value, str) and not value.strip():
            return
        if isinstance(value, list) and not value:
            return
        lines.append(f"{label}: {value}")

    def clean_item_line(raw: Any) -> tuple[str | None, str | None, str]:
        text = str(raw or "").strip()
        if not text:
            return None, None, ""
        parts = [p.strip() for p in text.split(",") if p.strip()]
        stage = None
        seq = None
        kept: List[str] = []
        for part in parts:
            lowered = part.lower()
            if lowered.startswith("stage:"):
                stage = part.split(":", 1)[1].strip().lower() or None
                continue
            if lowered.startswith("sequence:"):
                seq = part.split(":", 1)[1].strip() or None
                continue
            kept.append(part)
        return stage, seq, ", ".join(kept)

    STAGE_LABELS = {
        "setup": "SETUP",
        "tease": "TEASE",
        "build": "BUILD-UP",
        "climax": "CLIMAX",
        "after": "AFTERCARE",
    }
    STAGE_ORDER_OLDEST_FIRST = ["setup", "tease", "build", "climax", "after"]

    def format_numbered_block(items: List[Any], *, with_stage_groups: bool) -> None:
        if not items:
            lines.append("NONE")
            return

        if with_stage_groups:
            grouped: Dict[str, List[tuple[str | None, str]]] = {}
            for raw_item in items:
                stage, seq, cleaned = clean_item_line(raw_item)
                if not cleaned:
                    continue
                key = stage or "unknown"
                grouped.setdefault(key, []).append((seq, cleaned))

            ordered_stage_keys = [
                key for key in STAGE_ORDER_OLDEST_FIRST if key in grouped
            ] + [
                key
                for key in grouped.keys()
                if key not in set(STAGE_ORDER_OLDEST_FIRST) and key != "unknown"
            ]
            if "unknown" in grouped:
                ordered_stage_keys.append("unknown")

            counter = 0
            for stage_key in ordered_stage_keys:
                stage_label = STAGE_LABELS.get(stage_key, str(stage_key).upper())
                lines.append(f"{stage_label}")
                for seq, cleaned in grouped.get(stage_key, []):
                    counter += 1
                    lines.append(f"Content {counter}:")
                    if seq:
                        lines.append(f"seq {seq} — {cleaned}")
                    else:
                        lines.append(cleaned)
                    lines.append("")
            while lines and lines[-1] == "":
                lines.pop()
            return

        counter = 0
        for raw_item in items:
            _stage, seq, cleaned = clean_item_line(raw_item)
            if not cleaned:
                continue
            counter += 1
            lines.append(f"Content {counter}:")
            if seq:
                lines.append(f"seq {seq} — {cleaned}")
            else:
                lines.append(cleaned)
            lines.append("")
        while lines and lines[-1] == "":
            lines.pop()

    if zoom_level <= 0:
        add_header("SCRIPTS (newest → oldest)")
        scripts = pack.get("scripts") or []
        if isinstance(scripts, list) and scripts:
            for idx, script in enumerate(scripts, start=1):
                if not isinstance(script, dict):
                    continue
                title = (script.get("title") or "").strip()
                summary = (script.get("script_summary") or script.get("summary") or "").strip()
                core_inventory = script.get("core_inventory") or ""
                ammo_inventory = script.get("ammo_inventory") or ""
                ammo_summary = (script.get("ammo_summary") or "").strip()
                core_scene_time = (script.get("core_scene_time_of_day") or "").strip()

                lines.append(f"Script {idx}: {title or 'UNTITLED'}")
                if summary:
                    lines.append(f"Summary: {summary}")
                if core_scene_time:
                    lines.append(f"Core scene time: {core_scene_time}")
                if core_inventory:
                    lines.append(f"Core inventory: {core_inventory}")
                if ammo_inventory:
                    lines.append(f"Ammo inventory: {ammo_inventory}")
                if ammo_summary:
                    lines.append(f"Ammo summary: {ammo_summary}")
                lines.append("")
            while lines and lines[-1] == "":
                lines.pop()
        else:
            lines.append("NONE")

        global_ammo = pack.get("global_ammo") or {}
        if isinstance(global_ammo, dict):
            add_header("GLOBAL AMMO (scriptless)")
            inventory = global_ammo.get("inventory")
            breakdown = global_ammo.get("breakdown") or {}
            if inventory:
                lines.append(f"Inventory: {inventory}")
            if isinstance(breakdown, dict) and breakdown:
                lines.append("Breakdown:")
                for media_type in ["photo", "video", "voice", "text", "unknown"]:
                    val = breakdown.get(media_type)
                    if val:
                        lines.append(f"- {media_type}: {val}")

        global_focus = pack.get("global_focus") or {}
        if isinstance(global_focus, dict):
            focus_items = global_focus.get("items") or []
            if focus_items:
                add_header("SCRIPTLESS CONTENT (focus shelf)")
                format_numbered_block(list(focus_items), with_stage_groups=False)

        return "\n".join(lines).strip()

    if zoom_level == 1:
        script = pack.get("script") or {}
        add_header("SCRIPT OVERVIEW")
        if isinstance(script, dict):
            add_kv("Title", (script.get("title") or "").strip() or None)
            add_kv("Summary", (script.get("summary") or "").strip() or None)
            add_kv("Time", (script.get("time_of_day") or "").strip() or None)
            add_kv("Location", (script.get("location_primary") or "").strip() or None)
            add_kv("Outfit", (script.get("outfit_category") or "").strip() or None)
            focus_tags = script.get("focus_tags") or []
            if isinstance(focus_tags, list) and focus_tags:
                add_kv("Focus tags", ", ".join(str(t) for t in focus_tags if t))

        add_header("SCRIPT CONTENT (core items; SETUP → AFTERCARE)")
        format_numbered_block(list(pack.get("script_items") or []), with_stage_groups=True)

        add_header("SCRIPT EXTRAS (same shoot)")
        format_numbered_block(list(pack.get("shoot_extras") or []), with_stage_groups=False)

        global_focus = pack.get("global_focus") or {}
        focus_items = (global_focus.get("items") if isinstance(global_focus, dict) else None) or []
        add_header("SCRIPTLESS CONTENT (focus shelf)")
        format_numbered_block(list(focus_items), with_stage_groups=False)

        return "\n".join(lines).strip()

    # zoom >= 2 (detailed)
    add_header("SCRIPT CONTENT (detailed)")
    format_numbered_block(list(pack.get("items") or []), with_stage_groups=True)

    global_focus = pack.get("global_focus") or {}
    focus_items = (global_focus.get("items") if isinstance(global_focus, dict) else None) or []
    if focus_items:
        add_header("SCRIPTLESS CONTENT (focus shelf)")
        format_numbered_block(list(focus_items), with_stage_groups=False)

    return "\n".join(lines).strip()
