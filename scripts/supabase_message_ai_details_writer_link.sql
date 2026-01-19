/*
Add writer linkage fields to message_ai_details.
This lets you see the creator reply directly on the AI details row.
Safe to re-run.
*/

alter table public.message_ai_details
  add column if not exists creator_reply_message_id bigint,
  add column if not exists creator_reply_text text;
