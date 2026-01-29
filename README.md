# Workers
All the Workers/Prompts/Code?

Supabase credentials for view:

url: https://takibtvdaivmaavikqpi.supabase.co
anon key: eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InRha2lidHZkYWl2bWFhdmlrcXBpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjE5MjQ1MzUsImV4cCI6MjA3NzUwMDUzNX0.IMcFvAeF7A-9fR7-TJlM5yulK4dVQH_uW1-6rgUs8No

## Iris decisions (FULL/LITE/SKIP)

The reply pipeline is now routed by Iris (first) and should not break if some worker outputs are missing.

Key env flags:
- `IRIS_CONTROL_ENABLED=1`: downstream workers/join respect Iris routing.
- `IRIS_ALLOW_SKIP=1`: allow true SKIP decisions (otherwise SKIP is treated as LITE).
- `IRIS_LITE_AS_FULL=1` (default): treat Iris LITE as FULL while LITE prompts are evolving.

Hermes skip safety (to prevent collapse when Hermes is skipped):
- Reuse the last Hermes content menu for ~4 hours, then fall back to `zoom=0` (overview).
- Reuse the last Hermes identity filtering keys for ~4 hours (to keep prompts stable/small).

Config:
- `CONTENT_REQUEST_CARRY_FORWARD_SECONDS=14400`
- `IDENTITY_KEYS_CARRY_FORWARD_SECONDS=14400`

Supabase migrations (safe to re-run):
- `scripts/supabase_message_ai_details_iris_columns.sql`
- `scripts/supabase_message_ai_details_content_pack_columns.sql`
- `scripts/supabase_message_ai_details_iris_view.sql`
