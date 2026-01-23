import json
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

CURRENT_TIME_NOTE = (
    "THIS IS THE TIME THAT IS RIGHT NOW, AND THIS IS THE PRESENT AND YOU ARE "
    "OPERATING HERE AT THIS TIME IN THE DAILY PLAN."
)


def safe_now_local_iso(tz_name: str | None) -> tuple[str, str]:
    """
    Return (tz_name, now_local_iso). Fail-open to UTC on invalid/missing tz.
    """
    fallback_tz = "UTC"
    tz = (tz_name or "").strip() or fallback_tz
    try:
        now_local = datetime.now(ZoneInfo(tz))
        return tz, now_local.replace(second=0, microsecond=0).isoformat()
    except Exception:
        now_utc = datetime.now(timezone.utc)
        return fallback_tz, now_utc.replace(second=0, microsecond=0).isoformat()


def current_plan_date_for_now(now_local_iso: str) -> str | None:
    """
    Timeline day windows are 02:00 -> next day 02:00.
    If it's before 02:00 local, "today's" plan is yesterday's plan_date.
    """
    try:
        now_local = datetime.fromisoformat(now_local_iso)
    except Exception:
        return None
    plan_day = now_local.date()
    if now_local.hour < 2:
        from datetime import timedelta

        plan_day = plan_day - timedelta(days=1)
    return plan_day.isoformat()


def fetch_daily_plan_row(sb, thread_id: int) -> dict | None:
    """
    Return the most relevant daily_plans row for "now" (plan_date anchored at 02:00 local).
    Falls back to the newest plan when an exact match is missing.
    """
    rows = (
        sb.table("daily_plans")
        .select("plan_date,plan_json,raw_text,status,error,time_zone,generated_at")
        .eq("thread_id", int(thread_id))
        .order("plan_date", desc=True)
        .limit(5)
        .execute()
        .data
        or []
    )
    if not rows:
        return None

    newest = rows[0] if isinstance(rows[0], dict) else {}
    tz_name = newest.get("time_zone")
    if not tz_name and isinstance(newest.get("plan_json"), dict):
        tz_name = newest["plan_json"].get("time_zone")

    tz_name, now_local_iso = safe_now_local_iso(tz_name)
    target_plan_date = current_plan_date_for_now(now_local_iso)
    if not target_plan_date:
        return newest

    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get("plan_date") or "").strip() == target_plan_date:
            row["_computed_tz_name"] = tz_name
            row["_computed_now_local"] = now_local_iso
            return row

    # Try fetching exact date in case it's just outside the small window.
    try:
        exact = (
            sb.table("daily_plans")
            .select("plan_date,plan_json,raw_text,status,error,time_zone,generated_at")
            .eq("thread_id", int(thread_id))
            .eq("plan_date", target_plan_date)
            .limit(1)
            .execute()
            .data
            or []
        )
        if exact and isinstance(exact[0], dict):
            exact[0]["_computed_tz_name"] = tz_name
            exact[0]["_computed_now_local"] = now_local_iso
            return exact[0]
    except Exception:
        pass

    newest["_computed_tz_name"] = tz_name
    newest["_computed_now_local"] = now_local_iso
    return newest


def format_daily_plan_for_prompt(plan_row: dict | None) -> str:
    """
    Compact, Kairos-readable view of today's daily plan.
    Adds the CURRENT_TIME_NOTE into the segment that holds "now".
    """
    if not plan_row:
        return "NO_DAILY_PLAN: true"

    tz_name = plan_row.get("_computed_tz_name") or plan_row.get("time_zone") or ""
    now_local_iso = plan_row.get("_computed_now_local") or ""
    plan_date = str(plan_row.get("plan_date") or "").strip()
    status = str(plan_row.get("status") or "").strip() or "unknown"
    error = str(plan_row.get("error") or "").strip()
    generated_at = str(plan_row.get("generated_at") or "").strip()

    plan_json = plan_row.get("plan_json")
    if isinstance(plan_json, str):
        try:
            plan_json = json.loads(plan_json)
        except Exception:
            plan_json = None

    lines: list[str] = []
    lines.append(f"PLAN_DATE: {plan_date or 'unknown'}")
    lines.append(f"TIME_ZONE: {tz_name or 'unknown'}")
    if now_local_iso:
        lines.append(f"NOW_LOCAL: {now_local_iso}")
    if generated_at:
        lines.append(f"GENERATED_AT_UTC: {generated_at}")
    lines.append(f"STATUS: {status}")
    if error:
        lines.append(f"ERROR: {error}")

    if not isinstance(plan_json, dict):
        raw_text = (plan_row.get("raw_text") or "").strip()
        if raw_text:
            preview = raw_text[:2000]
            lines.append("RAW_TEXT_PREVIEW:")
            lines.append(preview)
        return "\n".join(lines).strip()

    segments: list[dict] = []
    hours = plan_json.get("hours")
    if isinstance(hours, list):
        for hour in hours:
            if not isinstance(hour, dict):
                continue
            segs = hour.get("segments")
            if not isinstance(segs, list):
                continue
            for seg in segs:
                if isinstance(seg, dict):
                    segments.append(seg)

    def _parse_local_dt(value: str) -> datetime | None:
        try:
            dt = datetime.fromisoformat(value)
        except Exception:
            return None
        if dt.tzinfo is not None:
            return dt
        if not tz_name:
            return dt.replace(tzinfo=timezone.utc)
        try:
            return dt.replace(tzinfo=ZoneInfo(str(tz_name)))
        except Exception:
            return dt.replace(tzinfo=timezone.utc)

    now_local_dt = _parse_local_dt(now_local_iso) if now_local_iso else None
    current_seg = None
    if now_local_dt:
        for seg in segments:
            start = _parse_local_dt(str(seg.get("start_local") or ""))
            end = _parse_local_dt(str(seg.get("end_local") or ""))
            if not start or not end:
                continue
            if start <= now_local_dt < end:
                current_seg = seg
                break

    if current_seg:
        existing_notes = str(current_seg.get("notes") or "").strip()
        if CURRENT_TIME_NOTE not in existing_notes:
            current_seg["notes"] = (
                f"{existing_notes} {CURRENT_TIME_NOTE}".strip()
                if existing_notes
                else CURRENT_TIME_NOTE
            )

    def _fmt_seg(seg: dict) -> str:
        start = str(seg.get("start_local") or "").strip()
        end = str(seg.get("end_local") or "").strip()
        state = str(seg.get("state") or "").strip() or "unspecified"
        label = str(seg.get("label") or "").strip() or "Unspecified"
        notes = str(seg.get("notes") or "").strip()
        source = str(seg.get("source") or "").strip()
        tail = []
        if source:
            tail.append(f"source={source}")
        if notes:
            tail.append(f"notes={notes}")
        suffix = f" ({', '.join(tail)})" if tail else ""
        return f"- {start} -> {end} | {state} | {label}{suffix}"

    if current_seg:
        lines.append("CURRENT_SEGMENT:")
        lines.append(_fmt_seg(current_seg))

    # Upcoming changes: next ~12 segments after now
    if now_local_dt:
        future: list[tuple[datetime, dict]] = []
        for seg in segments:
            start_raw = str(seg.get("start_local") or "")
            start = _parse_local_dt(start_raw)
            if not start:
                continue
            if start >= now_local_dt:
                future.append((start, seg))
        future.sort(key=lambda x: x[0])
        if future:
            lines.append("UPCOMING_SEGMENTS:")
            for _, seg in future[:12]:
                lines.append(_fmt_seg(seg))

    # Always include full segment list, but cap length.
    lines.append("FULL_DAY_SEGMENTS:")
    for seg in segments[:200]:
        lines.append(_fmt_seg(seg))

    out = "\n".join(lines).strip()
    return out[:12000]
