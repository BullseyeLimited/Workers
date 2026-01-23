/*
Rename the old Kairos alignment column to MOMENT_COMPASS.

Safe to re-run.

- If public.message_ai_details.alignment_status exists and moment_compass does not:
  - rename alignment_status -> moment_compass
  - coerce moment_compass to TEXT (freeform prose)
- If neither exists: add moment_compass TEXT
- If moment_compass already exists: no-op
*/

do $$
begin
  if exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'message_ai_details'
      and column_name = 'alignment_status'
  ) and not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'message_ai_details'
      and column_name = 'moment_compass'
  ) then
    alter table public.message_ai_details
      rename column alignment_status to moment_compass;

    -- Ensure the new column is plain text for normal-sentence analysis.
    alter table public.message_ai_details
      alter column moment_compass type text using moment_compass::text;

  elsif not exists (
    select 1
    from information_schema.columns
    where table_schema = 'public'
      and table_name = 'message_ai_details'
      and column_name = 'moment_compass'
  ) then
    alter table public.message_ai_details
      add column moment_compass text;
  end if;
end $$;

