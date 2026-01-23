/*
Add Kairos schedule rethink output storage to message_ai_details.

Safe to re-run.

Run in Supabase SQL editor.
*/

alter table public.message_ai_details
  add column if not exists schedule_rethink text;

