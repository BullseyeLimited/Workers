-- Content ingest: integrity + observability
-- Run in Supabase SQL Editor (safe to re-run).

-- -------------------------------------------------------------------
-- 1) Media reference dedupe (one row per Storage object)
-- -------------------------------------------------------------------

alter table public.content_items
  add column if not exists storage_bucket text,
  add column if not exists storage_path text;

-- Optional: backfill storage_bucket/storage_path from url_main for existing rows
-- Expected pattern:
-- https://<project>.supabase.co/storage/v1/object/public/<bucket>/<path>
with parsed as (
  select
    id,
    split_part(split_part(url_main, '/object/public/', 2), '/', 1) as bucket,
    regexp_replace(split_part(url_main, '/object/public/', 2), '^([^/]+)/', '') as path
  from public.content_items
  where url_main like '%/storage/v1/object/public/%'
    and (storage_bucket is null or storage_path is null)
)
update public.content_items c
set
  storage_bucket = p.bucket,
  storage_path = p.path
from parsed p
where c.id = p.id;

-- Enforce uniqueness for any row that has a bucket+path (NULLs are allowed for non-storage items)
-- NOTE: Postgres doesn't support `ADD CONSTRAINT IF NOT EXISTS`, so we guard via pg_constraint.
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'content_items_storage_unique'
      and conrelid = 'public.content_items'::regclass
  ) then
    alter table public.content_items
      add constraint content_items_storage_unique unique (storage_bucket, storage_path);
  end if;
end $$;

-- Helpful indexes for queue/sweeper queries
create index if not exists content_items_ingest_status_updated_idx
  on public.content_items (ingest_status, ingest_updated_at);

create index if not exists content_items_ingest_status_id_idx
  on public.content_items (ingest_status, id);

-- -------------------------------------------------------------------
-- 2) Observability columns (worker writes these when present)
-- -------------------------------------------------------------------

alter table public.content_items
  add column if not exists ingest_started_at timestamptz,
  add column if not exists ingest_completed_at timestamptz,
  add column if not exists ingest_duration_ms integer,
  add column if not exists ingest_worker_id text,
  add column if not exists ingest_model text,
  add column if not exists ingest_prompt text,
  add column if not exists ingest_error_details text;

-- Optional: enforce ingest_status values (run the SELECT first to confirm)
-- select ingest_status, count(*) from public.content_items group by ingest_status order by 2 desc;
do $$
begin
  if not exists (
    select 1
    from pg_constraint
    where conname = 'content_items_ingest_status_check'
      and conrelid = 'public.content_items'::regclass
  ) then
    alter table public.content_items
      add constraint content_items_ingest_status_check
      check (ingest_status in ('pending', 'processing', 'ok', 'failed')) not valid;
  end if;

  alter table public.content_items
    validate constraint content_items_ingest_status_check;
end $$;

-- -------------------------------------------------------------------
-- 3) (Optional but recommended) Make Storage triggers use storage_bucket/path
--    This keeps delete + dedupe stable even if you later switch to signed URLs.
-- -------------------------------------------------------------------

-- NOTE: Replace project_id if you ever clone to another Supabase project.
create or replace function public.handle_new_media()
returns trigger
language plpgsql
security definer
as $$
declare
  folder_handle text;
  fetched_creator_id bigint;
  public_url text;
  file_mime text;
  detected_type text;
  project_id text := 'takibtvdaivmaavikqpi';
begin
  if lower(new.bucket_id) in ('content') then
    folder_handle := split_part(new.name, '/', 1);

    select id into fetched_creator_id
    from public.creators
    where of_handle = folder_handle
    limit 1;

    if fetched_creator_id is not null then
      file_mime := new.metadata->>'mimetype';

      if file_mime like 'video%' then
        detected_type := 'video';
      elsif file_mime like 'image%' then
        detected_type := 'image';
      elsif file_mime like 'audio%' then
        detected_type := 'audio';
      else
        detected_type := 'other';
      end if;

      public_url := 'https://' || project_id || '.supabase.co/storage/v1/object/public/' || new.bucket_id || '/' || new.name;

      insert into public.content_items (
        creator_id,
        media_type,
        url_main,
        mimetype,
        ingest_status,
        created_at,
        storage_bucket,
        storage_path
      )
      values (
        fetched_creator_id,
        detected_type,
        public_url,
        file_mime,
        'pending',
        now(),
        new.bucket_id,
        new.name
      )
      on conflict (storage_bucket, storage_path) do nothing;
    end if;
  end if;
  return new;
end;
$$;

drop trigger if exists on_file_upload on storage.objects;
create trigger on_file_upload
  after insert on storage.objects
  for each row
  execute procedure public.handle_new_media();

create or replace function public.handle_deleted_media()
returns trigger
language plpgsql
security definer
as $$
declare
  project_id text := 'takibtvdaivmaavikqpi';
  public_url text;
begin
  if lower(old.bucket_id) in ('content') then
    public_url := 'https://' || project_id || '.supabase.co/storage/v1/object/public/' || old.bucket_id || '/' || old.name;

    delete from public.content_items
    where (storage_bucket = old.bucket_id and storage_path = old.name)
       or url_main = public_url
       or url_thumb = public_url;
  end if;
  return old;
end;
$$;

drop trigger if exists on_file_delete on storage.objects;
create trigger on_file_delete
  after delete on storage.objects
  for each row
  execute procedure public.handle_deleted_media();

-- -------------------------------------------------------------------
-- 4) Queue cleanup (optional): when a content_item is deleted, remove its queued ingest job
--    This prevents stale jobs when you delete media and the delete trigger removes content_items.
-- -------------------------------------------------------------------

create or replace function public.handle_content_item_deleted_queue_cleanup()
returns trigger
language plpgsql
security definer
as $$
begin
  delete from public.job_queue
  where queue = 'content.ingest'
    and (payload->>'content_id') = old.id::text;
  return old;
end;
$$;

drop trigger if exists on_content_item_delete_cleanup on public.content_items;
create trigger on_content_item_delete_cleanup
  after delete on public.content_items
  for each row
  execute procedure public.handle_content_item_deleted_queue_cleanup();
