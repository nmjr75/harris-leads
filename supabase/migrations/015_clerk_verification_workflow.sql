-- 015_clerk_verification_workflow.sql
-- Extend the verification workflow (originally probate-only via mig 012) to
-- clerk_lead and clerk_foreclosure records. Adds:
--
--   1. records.manual_submitted_at / manual_submitted_by — distinguish
--      VA-submitted records from legacy auto-queued submissions. Drives
--      the new "Sent to SIFTstack" vs "Auto-Q" tabs on the main dashboard.
--
--   2. Backfill verification_status for clerk records:
--      - records currently in siftstack_status (queued/completed/failed)
--        → leave verification_status null (they belong to Auto-Q tab,
--        independent of verification)
--      - records WITHOUT a property address → 'needs_research' (Research tab)
--      - all remaining clerk records → 'pending' (Verify tab)
--
--   3. Extend contact_phones.role check constraint to include 'owner' and
--      'co_borrower' so the new clerk skip-trace panel can attribute
--      phones to the correct person.

-- 1. Manual submission tracking ----------------------------------------------

alter table public.records
    add column if not exists manual_submitted_at  timestamptz,
    add column if not exists manual_submitted_by  uuid references auth.users(id);

comment on column public.records.manual_submitted_at is
    'Timestamp set when a VA explicitly submits this record to SIFTstack '
    'via the dashboard "Submit to SIFTstack" button. Null for records that '
    'were auto-queued by the scraper. Used to split the Auto-Q tab from '
    'the Sent to SIFTstack tab.';

comment on column public.records.manual_submitted_by is
    'auth.uid() of the VA who clicked Submit to SIFTstack on this record.';

create index if not exists records_manual_submitted_idx
    on public.records(manual_submitted_at)
    where manual_submitted_at is not null;

-- 2. Backfill verification_status for clerk records --------------------------

update public.records
   set verification_status = 'needs_research'
 where source in ('clerk_lead', 'clerk_foreclosure')
   and verification_status is null
   and siftstack_status is null
   and (prop_address is null or prop_address = '');

update public.records
   set verification_status = 'pending'
 where source in ('clerk_lead', 'clerk_foreclosure')
   and verification_status is null
   and siftstack_status is null;

-- Records with siftstack_status set are intentionally left with
-- verification_status null — the Auto-Q tab queries them by
-- siftstack_status, not by verification_status.

-- 3. Extend contact_phones.role to support clerk record contact types -------

-- Drop and recreate the check constraint with two new values added.
-- 'owner' = the named owner on the deed
-- 'co_borrower' = secondary borrower on the underlying loan
alter table public.contact_phones
    drop constraint if exists contact_phones_role_check;

alter table public.contact_phones
    add constraint contact_phones_role_check
    check (role in ('pr', 'deceased', 'heir', 'attorney', 'other',
                    'owner', 'co_borrower'));

-- Same for emails
alter table public.contact_emails
    drop constraint if exists contact_emails_role_check;

alter table public.contact_emails
    add constraint contact_emails_role_check
    check (role in ('pr', 'deceased', 'heir', 'attorney', 'other',
                    'owner', 'co_borrower'));

-- 4. Sanity check (run manually after migration applies) --------------------
-- select source, verification_status, siftstack_status, count(*)
--   from public.records
--  where source in ('clerk_lead', 'clerk_foreclosure')
--  group by 1, 2, 3
--  order by 1, 2, 3;
