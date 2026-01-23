/*
Add a "where is the creator right now?" state field to message_ai_details.

Safe to re-run.

Run in Supabase SQL editor.
*/

alter table public.message_ai_details
  add column if not exists moment_location text;

