-- =============================================================================
-- One-shot data fix: strip cat/cat_label from EstateTrace records
-- -----------------------------------------------------------------------------
-- The first probate sheet sync (commit 4d5161c) wrote cat='PROBATE' and
-- cat_label='Probate (EstateTrace)' on every imported row. That caused the
-- dashboard's main-table 'PROBATE' category card to mix existing clerk
-- probate-proceeds records with the new EstateTrace records — exactly the
-- mixing Nelson called out as unacceptable.
--
-- Fix:
--   1. Clear cat / cat_label on existing EstateTrace records (this script).
--   2. probate_sheet_sync.py now writes None for both fields going forward.
--   3. Dashboard now hides EstateTrace records from the default main-table
--      view; they only appear when the source filter is explicitly set to
--      'estatetrace', or via the new EstateTrace stat card.
--
-- After running this, the main-table 'PROBATE' card will show only the
-- existing clerk probate records (~893 expected); the EstateTrace count
-- (1,692 expected) shows in its own dedicated card.
--
-- This script is idempotent — safe to re-run.
-- =============================================================================

update public.records
   set cat = null,
       cat_label = null
 where source = 'estatetrace'
   and (cat is not null or cat_label is not null);

-- Sanity check: count by source after fix
select source, count(*) as records, count(cat) as still_has_cat
  from public.records
 group by 1
 order by 2 desc;
