-- =============================================================================
-- Migration 002 — Make match_confidence editable by editor role
-- =============================================================================
-- The original schema silently reverted any editor attempt to change
-- match_confidence. This migration:
--   1. Removes match_confidence from the "protected columns" list, so editors
--      can set it to high/medium/low/none after manually verifying addresses.
--   2. Adds match_confidence to the audit log so every change is tracked.
--   3. Extends the scraper's manual_overrides respect list so a VA's
--      confidence upgrade is never overwritten by the next scraper run.
--
-- Paste entire file into: Supabase → SQL Editor → New Query → Run.
-- =============================================================================

create or replace function public.log_record_changes()
returns trigger
language plpgsql
security definer
set search_path = public
as $$
declare
    v_source    text := 'manual';
    v_user_id   uuid := auth.uid();
    v_role      text := public.current_user_role();
    v_changed   text[] := array[]::text[];
begin
    if v_user_id is null then
        v_source := 'scraper';
    end if;

    -- Editor role: silently revert any change to non-editable columns.
    -- match_confidence is NOW editable (moved out of this list).
    if v_source = 'manual' and v_role = 'editor' then
        new.id              := old.id;
        new.doc_num         := old.doc_num;
        new.doc_type        := old.doc_type;
        new.cat             := old.cat;
        new.cat_label       := old.cat_label;
        new.county          := old.county;
        new.state           := old.state;
        new.filed_date      := old.filed_date;
        new.grantee_names   := old.grantee_names;
        new.mortgagee       := old.mortgagee;
        new.parcel_acct     := old.parcel_acct;
        new.clerk_url       := old.clerk_url;
        new.hcad_url        := old.hcad_url;
        new.pdf_url         := old.pdf_url;
        new.address_source  := old.address_source;
        new.flags           := old.flags;
        new.score           := old.score;
        new.batch_id        := old.batch_id;
        new.batch_label     := old.batch_label;
        new.date_scraped    := old.date_scraped;
        new.needs_ocr       := old.needs_ocr;
        new.created_at      := old.created_at;
    end if;

    -- Scraper: skip any field a human has manually set.
    if v_source = 'scraper' and old.manual_overrides is not null then
        if 'owner'             = any(old.manual_overrides) then new.owner := old.owner; end if;
        if 'co_borrower'       = any(old.manual_overrides) then new.co_borrower := old.co_borrower; end if;
        if 'grantor_name'      = any(old.manual_overrides) then new.grantor_name := old.grantor_name; end if;
        if 'legal_description' = any(old.manual_overrides) then new.legal_description := old.legal_description; end if;
        if 'prop_address'      = any(old.manual_overrides) then new.prop_address := old.prop_address; end if;
        if 'prop_city'         = any(old.manual_overrides) then new.prop_city := old.prop_city; end if;
        if 'prop_zip'          = any(old.manual_overrides) then new.prop_zip := old.prop_zip; end if;
        if 'mail_address'      = any(old.manual_overrides) then new.mail_address := old.mail_address; end if;
        if 'mail_city'         = any(old.manual_overrides) then new.mail_city := old.mail_city; end if;
        if 'mail_zip'          = any(old.manual_overrides) then new.mail_zip := old.mail_zip; end if;
        if 'amount'            = any(old.manual_overrides) then new.amount := old.amount; end if;
        if 'sale_date'         = any(old.manual_overrides) then new.sale_date := old.sale_date; end if;
        if 'match_confidence'  = any(old.manual_overrides) then new.match_confidence := old.match_confidence; end if;
    end if;

    new.updated_at := now();
    if v_source = 'manual' then
        new.updated_by := v_user_id;
    end if;

    if new.owner is distinct from old.owner then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'owner', old.owner, new.owner, v_user_id, v_source);
        v_changed := array_append(v_changed, 'owner');
    end if;
    if new.co_borrower is distinct from old.co_borrower then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'co_borrower', old.co_borrower, new.co_borrower, v_user_id, v_source);
        v_changed := array_append(v_changed, 'co_borrower');
    end if;
    if new.grantor_name is distinct from old.grantor_name then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'grantor_name', old.grantor_name, new.grantor_name, v_user_id, v_source);
        v_changed := array_append(v_changed, 'grantor_name');
    end if;
    if new.legal_description is distinct from old.legal_description then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'legal_description', old.legal_description, new.legal_description, v_user_id, v_source);
        v_changed := array_append(v_changed, 'legal_description');
    end if;
    if new.prop_address is distinct from old.prop_address then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_address', old.prop_address, new.prop_address, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_address');
    end if;
    if new.prop_city is distinct from old.prop_city then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_city', old.prop_city, new.prop_city, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_city');
    end if;
    if new.prop_zip is distinct from old.prop_zip then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'prop_zip', old.prop_zip, new.prop_zip, v_user_id, v_source);
        v_changed := array_append(v_changed, 'prop_zip');
    end if;
    if new.mail_address is distinct from old.mail_address then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_address', old.mail_address, new.mail_address, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_address');
    end if;
    if new.mail_city is distinct from old.mail_city then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_city', old.mail_city, new.mail_city, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_city');
    end if;
    if new.mail_zip is distinct from old.mail_zip then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'mail_zip', old.mail_zip, new.mail_zip, v_user_id, v_source);
        v_changed := array_append(v_changed, 'mail_zip');
    end if;
    if new.amount is distinct from old.amount then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'amount', old.amount, new.amount, v_user_id, v_source);
        v_changed := array_append(v_changed, 'amount');
    end if;
    if new.sale_date is distinct from old.sale_date then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'sale_date', old.sale_date::text, new.sale_date::text, v_user_id, v_source);
        v_changed := array_append(v_changed, 'sale_date');
    end if;
    if new.match_confidence is distinct from old.match_confidence then
        insert into public.record_history(record_id, field_name, old_value, new_value, changed_by, source)
        values (new.id, 'match_confidence', old.match_confidence, new.match_confidence, v_user_id, v_source);
        v_changed := array_append(v_changed, 'match_confidence');
    end if;

    if v_source = 'manual' and array_length(v_changed, 1) > 0 then
        new.manual_overrides := (
            select coalesce(array_agg(distinct x), array[]::text[])
            from unnest(coalesce(new.manual_overrides, array[]::text[]) || v_changed) x
        );
    end if;

    return new;
end;
$$;
