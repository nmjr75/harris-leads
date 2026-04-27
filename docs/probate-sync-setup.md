# Probate Sheet Sync — Setup Guide

This sync pulls EstateTrace probate data from a shared Google Sheet into the
Supabase `records` table. **Read-only** — the sync never writes to the sheet.

## What gets synced

- Workbook: "Harris County Probate - Miguel" (shared with you by the freelancer)
- Tabs matching pattern `{County}_County_TX_{YYYY}-{MM}` or with `-LAST_REVISION`
  suffix (currently: Harris + Fortbend, monthly tabs)
- Auto-discovers new monthly tabs as they appear (no config change needed
  when May / June / etc. roll over)
- LAST_REVISION versions overwrite un-suffixed versions when both exist for
  the same case_number

## Schedule

The workflow runs automatically:
- **6:30 AM CT** (before VA morning shift)
- **12:30 PM CT** (before VA afternoon shift)

Manual runs (two ways):
- **Dashboard button** (recommended for VAs): "&#x21bb; Sync Probates" in the
  toolbar. Editor or admin role required. VAs never need to touch GitHub.
- **GitHub UI** (admin fallback): Actions → "Probate Sheet Sync" → Run workflow.

## One-time setup (do this in order)

### 1. Create a Google Cloud service account (read-only)

a. Go to https://console.cloud.google.com/projectcreate
   and create a new project named e.g. `harris-leads-sync`. (Skip if you
   already have a project for this kind of thing.)

b. Enable the Sheets API for the project:
   - https://console.cloud.google.com/apis/library/sheets.googleapis.com
   - Click **Enable**

c. Create the service account:
   - https://console.cloud.google.com/iam-admin/serviceaccounts
   - **Create Service Account**
   - Name: `harris-leads-probate-sync`
   - Description: "Read-only access to EstateTrace probate sheet"
   - Click **Create and Continue**
   - **Skip** the "Grant this service account access to project" step
     (it doesn't need any project-level role — just sheet access)
   - Click **Done**

d. Generate a JSON key:
   - Click into the new service account
   - **Keys** tab → **Add Key** → **Create new key** → **JSON** → **Create**
   - Browser downloads `harris-leads-sync-xxxxx.json` — keep this file safe,
     it's the credential
   - **Copy the service account email** — looks like
     `harris-leads-probate-sync@harris-leads-sync.iam.gserviceaccount.com`

### 2. Share the Google Sheet with the service account (Viewer)

a. Open the EstateTrace probate sheet in your browser.
b. Click **Share** (top right).
c. Paste the service account email.
d. **Set role to Viewer** (NOT Editor, NOT Commenter — Viewer only).
e. **Uncheck** "Notify people" (the service account doesn't read email).
f. Click **Share**.

That's the security boundary. Even if our code tried to write to the sheet,
Google would reject it because the service account has Viewer role only.

### 3. Get the Sheet ID

The sheet ID is the long string in the URL:
```
https://docs.google.com/spreadsheets/d/THIS_PART_IS_THE_SHEET_ID/edit
```

Copy that string.

### 4. Add GitHub repository secrets

GitHub → harris-leads repo → **Settings** → **Secrets and variables** →
**Actions** → **New repository secret**.

Add two secrets:

| Secret name | Value |
|---|---|
| `PROBATE_SHEET_ID` | The Sheet ID copied in step 3 |
| `GOOGLE_SHEETS_SERVICE_ACCOUNT_JSON` | Open the JSON key file from step 1d, copy its **entire contents** (including the curly braces), paste as the secret value |

(`SUPABASE_URL` and `SUPABASE_SECRET_KEY` are already configured from the
existing scrapers — no new secrets needed for those.)

### 5. Apply the database migration

In Supabase → SQL Editor → New query, paste the contents of
`supabase/migrations/011_probate_sheet_sync.sql` and run it. This adds:

- `case_status` text — `active` / `pending` / `no_property_found`
- `additional_applicants` jsonb — for App 2+ data
- `sheet_tab_name` text — audit trail
- `sheet_synced_at` timestamptz — staleness queries
- `source` text — distinguishes `estatetrace` vs `clerk_lead` vs `clerk_foreclosure`

The migration also backfills existing records' `source` based on `doc_type`
so the new column is populated everywhere on day one.

### 5b. Set up the dashboard "Sync Probates" button (Edge Function + PAT)

**You can skip this section if you don't want VAs to be able to trigger
ad-hoc syncs.** The cron schedule (6:30 AM and 12:30 PM CT) covers the
primary workflow. This section is what makes the dashboard button work
without giving VAs GitHub access.

**Architecture:** the button calls a Supabase Edge Function. The Edge
Function verifies the caller's role (admin/editor) and triggers the
GitHub workflow using a Personal Access Token (PAT) that lives in
Supabase secrets — never in the browser.

#### 5b.1. Create a fine-grained GitHub PAT

a. Go to https://github.com/settings/personal-access-tokens/new
   (this is the **fine-grained** PAT page — different from "classic" PATs)

b. Token name: `harris-leads-probate-sync-dispatch`

c. Expiration: 1 year (set a calendar reminder to rotate)

d. Repository access: **Only select repositories** → choose `nmjr75/harris-leads`

e. Repository permissions:
   - **Actions: Read and write** (this is the only permission needed —
     it allows triggering workflow_dispatch but nothing else)
   - Leave everything else as "No access"

f. Click **Generate token** → copy the token starting with `github_pat_...`

This PAT can ONLY trigger workflows in `nmjr75/harris-leads`. It cannot
read your code, push commits, modify settings, or touch other repos.
Worst-case if it leaks: someone can spam-trigger workflows in this one
repo, which has its own concurrency lock and rate limits.

#### 5b.2. Install the Supabase CLI (if not installed)

```bash
npm install -g supabase
```

Verify: `supabase --version`

#### 5b.3. Link your local repo to the Supabase project

From the harris-leads directory:

```bash
supabase login
supabase link --project-ref oqkcmysnhgorjkinfamu
```

(That project-ref is from `https://oqkcmysnhgorjkinfamu.supabase.co`.)

#### 5b.4. Set Supabase secrets for the Edge Function

```bash
supabase secrets set GITHUB_DISPATCH_PAT="github_pat_xxxxxxxxxx_paste_yours_here"
supabase secrets set GITHUB_REPO="nmjr75/harris-leads"
supabase secrets set GITHUB_WORKFLOW_FILE="probate-sync.yml"
```

(`SUPABASE_URL` and `SUPABASE_SERVICE_ROLE_KEY` are auto-populated by
Supabase for Edge Functions — no manual setup needed.)

#### 5b.5. Deploy the Edge Function

```bash
supabase functions deploy trigger-probate-sync
```

This uploads `supabase/functions/trigger-probate-sync/index.ts` and
makes it callable at:
`https://oqkcmysnhgorjkinfamu.supabase.co/functions/v1/trigger-probate-sync`

#### 5b.6. Test the button

a. Open https://nmjr75.github.io/harris-leads (sign in)
b. Top toolbar → click **&#x21bb; Sync Probates**
c. Confirmation dialog: "Sync started — typically completes within 30-60
   seconds." Click OK to open the live workflow status, or Cancel to stay.
d. Within 30-60 seconds, new probates appear in the table

If the button shows an error:
- "Not signed in" → log in first
- "Only editors can sync" → your account needs role 'admin' or 'editor'
- "Rate limited — try again in Xs" → wait the displayed seconds (1/min/user)
- "GitHub API returned 404" → the PAT or repo name is wrong; recheck step 5b.4
- "GitHub API returned 401" → the PAT is invalid or expired; regenerate

### 6. First test run (dry run, recommended)

GitHub → Actions → **Probate Sheet Sync** → **Run workflow**:
- ✅ Check "Preview only — list what would be upserted, no DB writes"
- (Optional) Set tab filter to `Fortbend` to test with just one county first
- Click **Run workflow**

Watch the run logs. You should see something like:
```
Found 4 target tabs:
  - Fortbend_County_TX_2026-04 (draft)
  - Fortbend_County_TX_2026-04-LAST_REVISION (LAST_REVISION)
  - Harris_County_TX_2026-04 (draft)
  - Harris_County_TX_2026-04-LAST_REVISION (LAST_REVISION)
Tab 'Fortbend_County_TX_2026-04': 87 data rows
...
DRY RUN — preview (first 3 records):
{...}
DRY RUN — would upsert 412 records (skipping DB write)
```

Verify the preview rows look correct — `doc_num` is the case number,
`grantor_name` is the deceased, `owner` is the PR, `mail_address` is empty
for Fortbend rows, etc.

### 7. First real run

Same workflow, this time with dry_run unchecked. After it completes, query
Supabase to confirm:

```sql
select source, county, case_status, count(*)
from records
where source = 'estatetrace'
group by 1, 2, 3
order by 1, 2, 3;
```

You should see rows grouped by county (Harris / Fortbend) and status.

## What's protected from sync overwrites

The existing `manual_overrides` system protects VA edits from getting
clobbered by the next sync. Once a VA edits any of these fields on a
probate record, the sync will **never** overwrite that field on subsequent
runs:

- `owner`, `co_borrower`, `grantor_name`, `legal_description`
- `prop_address`, `prop_city`, `prop_zip`
- `mail_address`, `mail_city`, `mail_zip`
- `amount`, `sale_date`

This is critical for Fortbend: once a VA fills in `mail_address` (the PR
street found via skip-trace), the sync will never blank it out even if the
sheet still shows it empty.

Protection happens at the database trigger level (`log_record_changes` in
schema.sql) — there's no way to bypass it from the sync code.

## Troubleshooting

**Workflow fails with "PROBATE_SHEET_ID not set"**
→ Step 4 missed. Add the secret in repo settings.

**Workflow fails with "403 Forbidden" reading the sheet**
→ Step 2 missed or wrong email. Open the sheet's Share dialog and confirm
the service account email is listed as Viewer.

**No tabs found / "No tabs matched the pattern"**
→ Tab names changed. The pattern is
`{County}_County_TX_{YYYY}-{MM}[-LAST_REVISION]`. If EstateTrace renames
tabs, update `TAB_PATTERN` in `scraper/probate_sheet_sync.py`.

**Rows imported but `mail_address` is null on Fortbend rows**
→ Expected. Fortbend's source data doesn't include the PR street. The
`/probate?queue=fortbend_addresses` workflow (Phase B) is where VAs fill
this in via skip-trace tools.

**Spanish status `SIN PROPIEDAD ENCONTRADA` not translating**
→ The translation table is `STATUS_TRANSLATION` in
`scraper/probate_sheet_sync.py`. Add new mappings there.

## Verifying read-only access (audit trail)

Any time you want to confirm the sync hasn't touched the sheet:

- Open the sheet → **File** → **Version history** → **See version history**.
  The service account will never appear as an editor (only Miguel or
  whoever runs the EstateTrace scraper).

- Or in Google Cloud Console → IAM → Service Accounts → click the sync
  account → **Logs** tab. You'll only see API calls like
  `spreadsheets.values.batchGet` — never any `*.update` calls.

## Cost

Free. Google Sheets API has a generous read quota (300 reads/min per
project), and we read 4-12 tabs per run × 4 runs/day = ~50 reads/day.
