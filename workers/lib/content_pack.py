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


def _pg_array_literal(values: Iterable[str]) -> str:
    escaped = []
    for val in values:
        v = str(val).replace('"', '\\"')
        escaped.append(f'"{v}"')
    return "{" + ",".join(escaped) + "}"


def _apply_array_filter(query, column: str, values: List[str]):
    if not values:
        return query
    if hasattr(query, "contains"):
        return query.contains(column, values)
    return query.filter(column, "cs", _pg_array_literal(values))


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


def _fetch_rows(
    client,
    *,
    creator_id: int,
    time_of_day: List[str] | None = None,
    location_primary: str | None = None,
    outfit_category: str | None = None,
    body_focus: List[str] | None = None,
    limit: int | None = None,
) -> List[Dict[str, Any]]:
    base_query = (
        client.table("content_items")
        .select(
            "id,media_type,explicitness,desc_short,desc_long,voice_transcript,"
            "duration_seconds,time_of_day,location_primary,outfit_category,"
            "outfit_layers,location_tags,mood_tags,action_tags,body_focus,"
            "camera_angle,shot_type,lighting,set_id,shoot_id,sequence_position"
        )
        .eq("creator_id", creator_id)
    )
    if time_of_day:
        if len(time_of_day) == 1:
            base_query = base_query.eq("time_of_day", time_of_day[0])
        else:
            base_query = base_query.in_("time_of_day", time_of_day)
    if location_primary:
        base_query = base_query.eq("location_primary", location_primary)
    if outfit_category:
        base_query = base_query.eq("outfit_category", outfit_category)
    if body_focus:
        base_query = _apply_array_filter(base_query, "body_focus", body_focus)
    if limit:
        base_query = base_query.limit(int(limit))

    return base_query.execute().data or []


def build_content_index(
    client,
    *,
    creator_id: int,
    time_of_day: str | List[str] | None = None,
    body_focus: List[str] | str | None = None,
    include_relations: bool = False,
    limit: int | None = None,
) -> Dict[str, Any]:
    """
    Build a combined group + item index for Hermes.
    Returns zoom 1 items plus zoom 0 group summaries.
    """
    time_values = _normalize_list(time_of_day)
    body_values = _normalize_list(body_focus)
    rows = _fetch_rows(
        client,
        creator_id=creator_id,
        time_of_day=time_values,
        body_focus=body_values,
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
    body_focus: List[str] | str | None = None,
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
    time_values = _normalize_list(time_of_day)
    body_values = _normalize_list(body_focus)
    expand_media = _normalize_list(media_expand)

    rows = _fetch_rows(
        client,
        creator_id=creator_id,
        time_of_day=time_values,
        location_primary=location_primary,
        outfit_category=outfit_category,
        body_focus=body_values,
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
        media_type = row.get("media_type") or "unknown"
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
