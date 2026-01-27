/*
Iris decision visibility helper.

Safe to re-run.
Run in Supabase SQL editor.
*/

create or replace view public.v_iris_decisions as
select
  message_id,
  thread_id,
  (extras->'iris'->>'status') as iris_status,
  (extras->'iris'->>'created_at') as iris_created_at,
  (extras->'iris'->>'raw_output') as iris_output_raw,
  (extras->'iris'->'parsed') as iris_parsed
from public.message_ai_details
where extras ? 'iris';

