/*
Bubble 1 bootstrap: ensure schema exists for scripts + stage/sequence.

Run in Supabase SQL editor (safe to re-run).
*/

create extension if not exists pgcrypto;

-- -------------------------------------------------------------------
-- 1) content_scripts table (script-level metadata)
-- -------------------------------------------------------------------

create table if not exists public.content_scripts (
  id uuid primary key default gen_random_uuid(),
  creator_id bigint not null,
  shoot_id uuid not null,
  title text,
  summary text,
  time_of_day text,
  location_primary text,
  outfit_category text,
  focus_tags text[],
  script_summary text,
  ammo_summary text,
  meta jsonb,
  finalize_status text,
  finalize_error text,
  finalize_attempts integer,
  finalize_updated_at timestamptz,
  created_at timestamptz default now()
);

alter table public.content_scripts
  add column if not exists creator_id bigint,
  add column if not exists shoot_id uuid,
  add column if not exists title text,
  add column if not exists summary text,
  add column if not exists time_of_day text,
  add column if not exists location_primary text,
  add column if not exists outfit_category text,
  add column if not exists focus_tags text[],
  add column if not exists script_summary text,
  add column if not exists ammo_summary text,
  add column if not exists meta jsonb,
  add column if not exists finalize_status text,
  add column if not exists finalize_error text,
  add column if not exists finalize_attempts integer,
  add column if not exists finalize_updated_at timestamptz,
  add column if not exists created_at timestamptz;

-- -------------------------------------------------------------------
-- 2) content_items columns (core items get script_id; ammo gets shoot_id)
-- -------------------------------------------------------------------

alter table public.content_items
  add column if not exists script_id uuid,
  add column if not exists shoot_id uuid,
  add column if not exists stage text,
  add column if not exists sequence_position integer;

-- -------------------------------------------------------------------
-- 3) Helpful integrity/indexing
-- -------------------------------------------------------------------

do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'content_scripts_creator_shoot_unique'
      and conrelid = 'public.content_scripts'::regclass
  ) then
    alter table public.content_scripts
      add constraint content_scripts_creator_shoot_unique unique (creator_id, shoot_id);
  end if;
end $$;

create index if not exists content_items_script_id_idx
  on public.content_items (script_id);

create index if not exists content_items_shoot_id_idx
  on public.content_items (shoot_id);

create index if not exists content_items_sequence_position_idx
  on public.content_items (sequence_position);
