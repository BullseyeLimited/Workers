-- Fan sim cursor table (tracks last creator message per thread)
-- Run in Supabase SQL Editor (safe to re-run).

create table if not exists public.fan_sim_cursors (
  thread_id bigint primary key references public.threads(id) on delete cascade,
  last_creator_message_id bigint not null,
  updated_at timestamptz not null default now()
);

create index if not exists fan_sim_cursors_updated_at_idx
  on public.fan_sim_cursors (updated_at);
