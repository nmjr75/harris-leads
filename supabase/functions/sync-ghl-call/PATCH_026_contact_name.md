# sync-ghl-call patch — populate `contact_name` (migration 026)

The deployed `sync-ghl-call` Edge Function lives in Supabase (was created via
the dashboard UI, not in this repo). This patch describes the minimal change
needed to populate `ghl_calls.contact_name` so the Call Activity drill-down
stops rendering "Unknown contact" for callers who aren't in
`DATA.leads`/`DATA.hitList`/`DATA.assignments`.

## Two-step change in REI Reply

The `Sync Calls to Lead Audit Dashboard` workflow's webhook body needs to
include the contact's full name. Add this line to the JSON body (anywhere
inside the top-level object):

```
"contact_full_name": "{{contact.full_name}}"
```

Optionally also send `contact_id` if it isn't there already:

```
"contact_id": "{{contact.id}}"
```

(`{{contact.id}}` and `{{contact.full_name}}` are global merge fields in GHL
workflows — they resolve to the workflow's contact context, independent of
the `phoneCall.*` Call Details namespace.)

## Edge Function diff

In the body parser, read the new field:

```ts
const contactFullName: string | null =
  body?.contact_full_name?.trim() || null;
```

In the upsert payload to `ghl_calls`, add the column:

```ts
const row = {
  // ... existing fields (started_at, ended_at, duration_seconds,
  // direction, status, agent_email, agent_name, contact_id, ...)
  contact_name: contactFullName,
};
```

That's the entire functional change.

## Apply migration before deploying the function

```sh
# In Supabase SQL Editor:
\i supabase/migrations/026_ghl_calls_contact_name.sql
# or paste the file contents into a new query and run.
```

Order matters: the column must exist before the updated function tries to
write to it. If the function deploys first, every subsequent webhook will
PostgREST-fail until the migration runs.

## Backfill (optional)

For existing call rows synced before this column existed, a one-shot
backfill could be run later:

```sql
-- Pull names from any contact-keyed map that has them (lead/hitList side).
-- Not strictly needed — the dashboard now falls back to resolveContactName()
-- for rows whose contact_name is null, and resolveContactName() includes
-- DATA.calls in its lookup chain. Future dialed-only contacts get the name
-- inline; older rows resolve via the in-memory chain. Backfill only if a
-- particular older row keeps showing "Unknown".
```
