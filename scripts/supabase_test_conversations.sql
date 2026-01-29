-- Test conversation harness
-- Stores only fan/creator turns for test threads.

create table if not exists public.test_conversations (
  id bigserial primary key,
  thread_id bigint not null references public.threads(id),
  sender text not null check (sender in ('fan','creator')),
  message_text text not null,
  media_payload jsonb,
  status text not null default 'pending' check (status in ('pending','sent','mirrored','error')),
  source_message_id bigint,
  meta jsonb,
  created_at timestamptz not null default now(),
  processed_at timestamptz
);

create index if not exists test_conversations_thread_id_idx
  on public.test_conversations (thread_id);

create index if not exists test_conversations_status_idx
  on public.test_conversations (status);

create index if not exists test_conversations_created_at_idx
  on public.test_conversations (created_at);

create unique index if not exists test_conversations_source_message_id_uniq
  on public.test_conversations (source_message_id)
  where source_message_id is not null;
