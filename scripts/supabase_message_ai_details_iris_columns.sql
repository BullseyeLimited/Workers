/*
Add dedicated Iris decision columns to message_ai_details.

Safe to re-run.
Run in Supabase SQL editor.
*/

alter table public.message_ai_details
  add column if not exists iris_status text;

alter table public.message_ai_details
  add column if not exists iris_error text;

alter table public.message_ai_details
  add column if not exists iris_created_at timestamptz;

alter table public.message_ai_details
  add column if not exists iris_output_raw text;

alter table public.message_ai_details
  add column if not exists iris_hermes_mode text;

alter table public.message_ai_details
  add column if not exists iris_hermes_reason text;

alter table public.message_ai_details
  add column if not exists iris_kairos_mode text;

alter table public.message_ai_details
  add column if not exists iris_kairos_reason text;

alter table public.message_ai_details
  add column if not exists iris_napoleon_mode text;

alter table public.message_ai_details
  add column if not exists iris_napoleon_reason text;

