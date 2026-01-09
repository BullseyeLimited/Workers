import json
from typing import Any, Dict, Iterable, List, Optional, Tuple


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
    "video": "video",
    "videos": "video",
    "audio": "voice",
    "voice": "voice",
    "voice_note": "voice",
    "voice_notes": "voice",
    "voicenote": "voice",
    "voicenotes": "voice",
    "sound": "voice",
    "text": "text",
}

TIME_ANY_ALIASES = {"any", "anytime", "all", "any_time"}


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


def _normalize_time_values(value) -> List[str]:
    raw = _normalize_list(value)
    normalized: List[str] = []
    for item in raw:
        lowered = item.strip().lower()
        if lowered in TIME_ANY_ALIASES:
            token = "anytime"
        else:
            token = item.strip()
        if token and token not in normalized:
            normalized.append(token)
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


def _apply_time_filter(query, time_values: List[str]):
    if not time_values:
        return query
    values = list(time_values)
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


def _parse_group_key(group_key: Optional[str]) -> Optional[Tuple[str, str, str]]:
    if not group_key:
        return None
    if not isinstance(group_key, str):
        group_key = str(group_key)
    parts = group_key.split("|", 2)
    if len(parts) != 3:
        return None
    return parts[0] or None, parts[1] or None, parts[2] or None


def _sort_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
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
    time_of_day: List[str] | None = None,
    location_primary: str | None = None,
    outfit_category: str | None = None,
    body_focus: List[str] | None = None,
    body_focus_mode: str | None = None,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    base_query = (
        client.table("content_items")
        .select(
            "id,media_type,explicitness,desc_short,desc_long,voice_transcript,"
            "duration_seconds,time_of_day,location_primary,outfit_category,"
            "outfit_layers,location_tags,mood_tags,action_tags,body_focus,"
            "camera_angle,shot_type,lighting,set_id,shoot_id,sequence_position,"
            "created_at"
        )
        .eq("creator_id", creator_id)
    )
    if time_of_day:
        base_query = _apply_time_filter(base_query, time_of_day)
    if location_primary:
        base_query = base_query.eq("location_primary", location_primary)
    if outfit_category:
        base_query = base_query.eq("outfit_category", outfit_category)
    if body_focus:
        mode = body_focus_mode or "any"
        base_query = _apply_array_filter(base_query, "body_focus", body_focus, mode=mode)
    if limit:
        base_query = base_query.limit(int(limit))

    rows = base_query.execute().data or []
    return _sort_rows(rows)


def build_content_index(
    client,
    *,
    creator_id: int,
    time_of_day: str | List[str] | None = None,
    body_focus: List[str] | str | None = None,
    body_focus_mode: str | None = None,
    include_relations: bool = False,
    limit: int | None = None,
) -> Dict[str, Any]:
    """
    Build a combined group + item index for Hermes.
    Returns zoom 1 items plus zoom 0 group summaries.
    """
    time_values = _normalize_time_values(time_of_day)
    body_values = _normalize_list(body_focus)
    rows = _fetch_rows(
        client,
        creator_id=creator_id,
        time_of_day=time_values,
        body_focus=body_values,
        body_focus_mode=body_focus_mode,
        limit=limit,
    )
    groups = _build_group_pack(rows, time_values)
    items = _build_item_pack(rows, [], include_relations, client)
    return {
        "zoom": 1,
        "filters": {"time_of_day": time_values, "body_focus": body_values},
        "groups": groups.get("groups", []),
        "items": items,
    }


def build_content_pack(
    client,
    *,
    creator_id: int,
    zoom: int | str = 0,
    time_of_day: str | List[str] | None = None,
    location_primary: str | None = None,
    outfit_category: str | None = None,
    group_key: str | None = None,
    body_focus: List[str] | str | None = None,
    body_focus_mode: str | None = None,
    media_expand: List[str] | str | None = None,
    include_relations: bool = False,
    limit: int | None = None,
) -> Dict[str, Any]:
    """
    Build a content pack for the given zoom level.
    Zoom 0: group counts
    Zoom 1: all items in a group (compact)
    Zoom 2: requested media expanded (others compact)
    """

    zoom_level = _safe_int(zoom, 0) or 0
    time_values = _normalize_time_values(time_of_day)
    body_values = _normalize_list(body_focus)
    expand_media = _normalize_media_types(_normalize_list(media_expand))

    if group_key:
        parsed = _parse_group_key(group_key)
        if parsed:
            group_time, group_location, group_outfit = parsed
            if not time_values and group_time:
                time_values = [group_time]
            if not location_primary and group_location:
                location_primary = group_location
            if not outfit_category and group_outfit:
                outfit_category = group_outfit

    rows = _fetch_rows(
        client,
        creator_id=creator_id,
        time_of_day=time_values,
        location_primary=location_primary,
        outfit_category=outfit_category,
        body_focus=body_values,
        body_focus_mode=body_focus_mode,
        limit=limit,
    )

    if zoom_level <= 0:
        return _build_group_pack(rows, time_values)

    items = _build_item_pack(rows, expand_media, include_relations, client)
    return {
        "zoom": zoom_level,
        "filters": {
            "time_of_day": time_values,
            "location_primary": location_primary,
            "outfit_category": outfit_category,
            "body_focus": body_values,
            "media_expand": expand_media,
            "body_focus_mode": body_focus_mode or "any",
        },
        "items": items,
    }


def _build_group_pack(rows: List[Dict[str, Any]], time_values: List[str]) -> Dict[str, Any]:
    groups: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        time_of_day = row.get("time_of_day") or "anytime"
        location = row.get("location_primary") or "unknown"
        outfit = row.get("outfit_category") or "unknown"
        key = f"{time_of_day}|{location}|{outfit}"

        group = groups.get(key)
        if group is None:
            group = {
                "group_key": key,
                "time_of_day": time_of_day,
                "location_primary": location,
                "outfit_category": outfit,
                "counts": {},
            }
            groups[key] = group

        media_type = row.get("media_type") or "unknown"
        media_type = _normalize_media_type(media_type) or "unknown"
        explicitness = row.get("explicitness") or "unknown"
        media_counts = group["counts"].setdefault(media_type, {})
        media_counts[explicitness] = media_counts.get(explicitness, 0) + 1
        media_counts["total"] = media_counts.get("total", 0) + 1

    # Preserve input order for stability
    ordered_groups = [groups[k] for k in groups.keys()]
    return {
        "zoom": 0,
        "filters": {"time_of_day": time_values},
        "groups": ordered_groups,
    }


def _build_item_pack(
    rows: List[Dict[str, Any]],
    expand_media: List[str],
    include_relations: bool,
    client,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for row in rows:
        media_type = _normalize_media_type(row.get("media_type")) or "unknown"
        detail = media_type in expand_media
        item = {
            "id": row.get("id"),
            "media_type": media_type,
            "explicitness": row.get("explicitness"),
            "duration_seconds": row.get("duration_seconds"),
            "desc_short": row.get("desc_short"),
            "desc_long": row.get("desc_long") if detail else None,
            "voice_transcript": row.get("voice_transcript") if detail else None,
            "voice_excerpt": _voice_excerpt(row.get("voice_transcript")),
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
            "set_id": row.get("set_id"),
            "shoot_id": row.get("shoot_id"),
            "sequence_position": row.get("sequence_position"),
            "detail_level": "full" if detail else "compact",
        }
        items.append(item)

    if include_relations and items:
        _attach_relation_counts(items, client)

    return items


def _attach_relation_counts(items: List[Dict[str, Any]], client) -> None:
    ids = [item.get("id") for item in items if item.get("id") is not None]
    if not ids:
        return

    rel_rows = (
        client.table("content_relations")
        .select("from_content_id,relation")
        .in_("from_content_id", ids)
        .execute()
        .data
        or []
    )

    counts: Dict[int, Dict[str, int]] = {}
    for row in rel_rows:
        from_id = row.get("from_content_id")
        rel = row.get("relation") or "unknown"
        if from_id is None:
            continue
        bucket = counts.setdefault(from_id, {})
        bucket[rel] = bucket.get(rel, 0) + 1

    for item in items:
        item_id = item.get("id")
        if item_id is None:
            continue
        item["relation_counts"] = counts.get(item_id, {})


def format_content_pack(pack: Dict[str, Any]) -> str:
    if not pack:
        return ""
    return json.dumps(pack, ensure_ascii=False)
