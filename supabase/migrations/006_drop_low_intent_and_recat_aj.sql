-- 006_drop_low_intent_and_recat_aj.sql
-- 1) Remove historical records for document types that are not high-intent
--    seller signals: REL (release of lien / DOT / judgment — anti-signal),
--    BNKRCY (active bankruptcy — automatic stay forbids solicitation),
--    JUDGE (generic small-claims judgment — too noisy).
-- 2) Reclassify A/J (Abstract of Judgment) from the legacy shared "JUD" cat
--    into its own "AOJ" cat now that JUDGE is gone, so the dashboard tile
--    label reflects only Abstracts of Judgment.

delete from public.records
 where doc_type in ('REL', 'BNKRCY', 'JUDGE');

update public.records
   set cat = 'AOJ'
 where doc_type = 'A/J'
   and cat is distinct from 'AOJ';
