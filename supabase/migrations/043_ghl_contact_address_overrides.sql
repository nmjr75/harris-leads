-- 043_ghl_contact_address_overrides.sql
-- Address rows keyed by contact_id, for contacts that don't have a row in
-- ghl_conversation_snapshots (and therefore aren't covered by the existing
-- ghl_latest_contact_address view). Primarily: Hit List slim contacts who
-- aged out of the recent-100 conversation snapshot window.
--
-- Populated by the new enrich-hitlist-contacts Edge Function: iterates every
-- contact at the SELLER DISPOSITION → HIT LIST stage, fetches their address
-- from the GHL Contacts API, and upserts a row here. The dashboard merges
-- this table into DATA.contactAddressByContact alongside the snapshot view,
-- so fullAddressFor() resolves an address for these contacts and the row
-- renders with the same address + property chips a scored lead gets.
--
-- The same enrich function also dispatches a GitHub repository_dispatch to
-- the siftmap-enricher-on-demand workflow per contact_id, which fills the
-- property_enrichment table (beds, baths, sqft, year built, est value,
-- equity %, market status). Those chips are then keyed off the normalized
-- address and pick up automatically.

create table if not exists public.ghl_contact_address_overrides (
  contact_id   text primary key,
  address      text,
  city         text,
  state        text,
  postal_code  text,
  source       text not null default 'ghl_api',
  updated_at   timestamptz not null default now()
);

create index if not exists ghl_contact_address_overrides_updated_idx
  on public.ghl_contact_address_overrides(updated_at desc);

alter table public.ghl_contact_address_overrides enable row level security;

-- Read: every authenticated user can read addresses (no PII here that isn't
-- already shown elsewhere on the dashboard).
drop policy if exists "address overrides read all auth"
  on public.ghl_contact_address_overrides;
create policy "address overrides read all auth"
  on public.ghl_contact_address_overrides
  for select to authenticated using (true);

-- Write: only the Edge Function's service_role key writes. VAs can't poke
-- in addresses from the dashboard.
