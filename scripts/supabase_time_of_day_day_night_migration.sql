/*
Migration: collapse time_of_day to 3 values: day | night | anytime

Run in Supabase SQL editor.

Notes:
- This updates existing rows that may still contain legacy values (morning/afternoon/evening).
- It is safe to run multiple times (idempotent).
*/

do $$
begin
  if to_regclass('public.content_items') is not null then
    update public.content_items
    set time_of_day = case
      when lower(trim(time_of_day)) in ('day', 'daytime', 'day_time', 'day light', 'daylight', 'morning', 'afternoon', 'noon')
        then 'day'
      when lower(trim(time_of_day)) in ('night', 'nighttime', 'night_time', 'evening', 'sunset', 'twilight', 'dusk')
        then 'night'
      when lower(trim(time_of_day)) in ('anytime', 'any', 'unspecified', 'unknown')
        then 'anytime'
      else time_of_day
    end
    where time_of_day is not null
      and lower(trim(time_of_day)) in (
        'day', 'daytime', 'day_time', 'day light', 'daylight', 'morning', 'afternoon', 'noon',
        'night', 'nighttime', 'night_time', 'evening', 'sunset', 'twilight', 'dusk',
        'anytime', 'any', 'unspecified', 'unknown'
      );
  end if;

  if to_regclass('public.content_scripts') is not null then
    update public.content_scripts
    set time_of_day = case
      when lower(trim(time_of_day)) in ('day', 'daytime', 'day_time', 'day light', 'daylight', 'morning', 'afternoon', 'noon')
        then 'day'
      when lower(trim(time_of_day)) in ('night', 'nighttime', 'night_time', 'evening', 'sunset', 'twilight', 'dusk')
        then 'night'
      when lower(trim(time_of_day)) in ('anytime', 'any', 'unspecified', 'unknown')
        then 'anytime'
      else time_of_day
    end
    where time_of_day is not null
      and lower(trim(time_of_day)) in (
        'day', 'daytime', 'day_time', 'day light', 'daylight', 'morning', 'afternoon', 'noon',
        'night', 'nighttime', 'night_time', 'evening', 'sunset', 'twilight', 'dusk',
        'anytime', 'any', 'unspecified', 'unknown'
      );
  end if;
end
$$;

