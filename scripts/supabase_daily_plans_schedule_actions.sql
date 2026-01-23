/*
Add optional schedule-action audit columns to daily_plans.

These are used when Napoleon applies a schedule patch (cancel/rename) after a
Kairos SCHEDULE_RETHINK=YES turn.

Safe to re-run.

Run in Supabase SQL editor.
*/

alter table public.daily_plans
  add column if not exists schedule_edits jsonb not null default '[]'::jsonb,
  add column if not exists schedule_last_action text,
  add column if not exists schedule_last_reasoning text,
  add column if not exists schedule_last_new_name text,
  add column if not exists schedule_last_message_id bigint,
  add column if not exists schedule_last_applied_at timestamptz,
  add column if not exists schedule_last_target jsonb;

