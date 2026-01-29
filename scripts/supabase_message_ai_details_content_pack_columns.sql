/*
Ensure message_ai_details has the columns needed for the Hermes Content Pack flow.

Safe to re-run.
Run in Supabase SQL editor.
*/

alter table public.message_ai_details
  add column if not exists content_request jsonb;

alter table public.message_ai_details
  add column if not exists content_pack jsonb;

alter table public.message_ai_details
  add column if not exists content_pack_status text;

alter table public.message_ai_details
  add column if not exists content_pack_error text;

alter table public.message_ai_details
  add column if not exists content_pack_created_at timestamptz;

