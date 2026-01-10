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


def _is_nullish(value) -> bool:
    return value is None or value == ""


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


def _count_by_stage(rows: List[Dict[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        stage = (row.get("stage") or "").strip().lower() or "unknown"
        counts[stage] = counts.get(stage, 0) + 1
    return counts


def _make_pack_ref(value: str | None) -> str | None:
    if not value:
        return None
    seed = str(value).strip()
    if not seed:
        return None
    digest = hashlib.blake2s(seed.encode("utf-8"), digest_size=5).hexdigest()
    return f"pack_{digest}"


def _totals_by_media(counts: Any) -> Dict[str, int]:
    """
    Reduce nested {media_type: {explicitness: n, total: n}} into {media_type: total}.
    """
    totals: Dict[str, int] = {}
    if not isinstance(counts, dict):
        return totals
    for media_type, bucket in counts.items():
        if not isinstance(bucket, dict):
            continue
        total = bucket.get("total")
        if isinstance(total, int) and total > 0:
            totals[str(media_type)] = total
    return totals


def _totals_by_explicitness(counts: Any) -> Dict[str, int]:
    """
    Reduce nested {media_type: {explicitness: n}} into {explicitness: total}.
    """
    totals: Dict[str, int] = {}
    if not isinstance(counts, dict):
        return totals
    for bucket in counts.values():
        if not isinstance(bucket, dict):
            continue
        for explicitness, value in bucket.items():
            if explicitness == "total":
                continue
            if isinstance(value, int) and value > 0:
                totals[str(explicitness)] = totals.get(str(explicitness), 0) + value
    if totals:
        totals["total"] = sum(totals.values())
    return totals


def _stages_present(stage_counts: Any) -> List[str]:
    if not isinstance(stage_counts, dict):
        return []
    order = ["setup", "tease", "build", "climax", "after"]
    seen: set[str] = set()
    stages: List[str] = []
    for stage in order:
        val = stage_counts.get(stage)
        if isinstance(val, int) and val > 0:
            stages.append(stage)
            seen.add(stage)
    extras = sorted(
        str(k)
        for k, v in stage_counts.items()
        if k not in seen and isinstance(v, int) and v > 0
    )
    stages.extend(extras)
    return stages


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
            "stage_counts": _count_by_stage(script_items),
            "extras_counts": _count_by_media_and_explicitness(extras),
            "total_steps": max(
                [r.get("sequence_position") or 0 for r in script_items] or [0]
            ),
        }
        if include_focus_tags:
            entry["focus_tags"] = script.get("focus_tags") or []
        index.append(entry)

    # Stable order: by created_at then script_id (so UI doesn't jump).
    def sort_key(row: Dict[str, Any]):
        return (row.get("created_at") or "", row.get("script_id") or "")

    return sorted(index, key=sort_key)


def build_content_index(
    client,
    *,
    creator_id: int,
    include_items: bool = True,
) -> Dict[str, Any]:
    """
    Script-first index for Hermes:
    - scripts: list of script summaries + counts
    - items: full compact list of content_items (so Hermes is all-knowing)
    """
    scripts = _fetch_scripts(client, creator_id=creator_id)
    items_rows: List[Dict[str, Any]] = []
    if include_items:
        items_rows = _fetch_rows(client, creator_id=creator_id)

    return {
        "zoom": 1,
        "scripts": _build_script_index(scripts, items_rows, include_focus_tags=True),
        "items": _build_item_pack(items_rows, expand_media=[]),
    }


def build_content_pack(
    client,
    *,
    creator_id: int,
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
    scripts = _fetch_scripts(client, creator_id=creator_id)
    # For Zoom 0 we return script list only (no items).
    if zoom_level <= 0 or not script_id:
        # We still need item rows to compute counts; fetch once.
        items_rows = _fetch_rows(client, creator_id=creator_id)
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

            stage_counts = entry.pop("stage_counts", {})
            entry["stages"] = _stages_present(stage_counts)
            entry["ammo_summary"] = (
                meta.get("ammo_summary")
                or meta.get("ammo_blurb")
                or _auto_ammo_summary(
                    title=entry.get("title"),
                    ammo_inventory=ammo_inventory,
                    core_scene_time_of_day=core_scene_time_of_day,
                )
            )
        return {
            "zoom": 0,
            "scripts": scripts_index,
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
