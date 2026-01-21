-- AI raw response capture
-- Run in Supabase SQL Editor (safe to re-run).

create table if not exists public.ai_raw_responses (
  id bigserial primary key,
  created_at timestamptz not null default now(),
  worker text not null,
  thread_id bigint,
  message_id bigint,
  run_id uuid,
  model text,
  request_json jsonb,
  response_json jsonb not null,
  status text,
  error text
);

create index if not exists ai_raw_responses_thread_id_idx
  on public.ai_raw_responses (thread_id);

create index if not exists ai_raw_responses_message_id_idx
  on public.ai_raw_responses (message_id);

create index if not exists ai_raw_responses_worker_idx
  on public.ai_raw_responses (worker);

create index if not exists ai_raw_responses_created_at_idx
  on public.ai_raw_responses (created_at);
