/*
Migration: remove shot_type + lighting columns (no longer used).

Run in Supabase SQL editor.

If you have views/functions that reference these columns, Postgres may block the drop.
In that case, drop/update the dependent objects first, or add `cascade` to the DROP COLUMN.
*/

alter table public.content_items
  drop column if exists shot_type,
  drop column if exists lighting;

alter table public.content_scripts
  drop column if exists shot_type,
  drop column if exists lighting;

