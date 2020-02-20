WITH event_d2c AS (with consults as (
        select
        distinct foi.order_id,
        med_id,
        last_claim_medid_approved,
        (foi.subscription_id is not null) as is_subs,
        (foi.consultation_id is not null) as is_telemed,
        foi.consultation_id,
        c.canceled_timestamp,
        c.consultation_created_on,
        c.visit_referred_timestamp,
        c.visit_submitted_timestamp,
        c.visit_signed_timestamp,
        c.prescription_added_timestamp,
        c.prescription_denied_timestamp
        from dwh.fact_order_item foi
        left join dwh.fact_consultation c
        on c.consultation_id = foi.consultation_id
        where foi.consultation_id is not null
    )
    select
    global_session_id,
    timestamp_main,
    session_start_time,
    type_main,
    touchpoint1,
    detail1,
    detail2,
    detail3,
    detail4,
    detail5,
    session_attributed_marketing_channel_group_type,
    session_attributed_generic_name_short,
    session_attributed_med_id,
    session_attributed_telemed_available,
    session_attributed_subs_available,
    session_attributed_custom_therapeutic_class,
    path,
    personid,
    device,
    platform,
    landing_page_path,
    landing_page_category,
    purchasing_patient,
    existing_account,
    max(case when path ilike '%subs%' then 1 else 0 end) over (partition by global_session_id) as session_has_entered_subs_funnel,
    max(case when path ilike '%treatments%' then 1 else 0 end) over (partition by global_session_id) as session_has_entered_cdp,
    c.is_subs as script_is_subs,
    c.is_telemed as script_is_telemed,
    max(script_is_subs::int) over (partition by global_session_id) as session_purchased_subs,
    max(script_is_telemed::int) over (partition by global_session_id) as session_purchased_telemed,
    c.consultation_id,
    c.canceled_timestamp,
    c.consultation_created_on,
    c.visit_referred_timestamp,
    c.visit_submitted_timestamp,
    c.visit_signed_timestamp,
    c.prescription_added_timestamp,
    c.prescription_denied_timestamp,
    atc.is_subs as atc_is_subs,
    med.telemed_start_date,
    med.telemed_end_date,
    med.subscription_start_date,
    med.subscription_end_date,
    case when session_start_time > dim_user.first_telemed_subs_order_timestamp then TRUE else FALSE end as purchasing_subs_patient
    from journey.event e
    left join consults c
    on e.detail2 = c.order_id and
        ((e.touchpoint1 = 'purchased_product' and c.med_id = e.detail3) or
        (e.touchpoint1 = 'filled_product' and c.last_claim_medid_approved = e.detail3))
    left join (
        select id, is_subs from progressive_web_prod.added_product union
        select id, is_subs from rx_web_prod_2.added_product
    ) atc on atc.id = e.segment_id and e.touchpoint1 = 'added_product'
    left join (
        select
            medid,
            telemed_start_date,
            telemed_end_date,
            subscription_start_date,
            subscription_end_date
        from dwh.dim_medid_hierarchy
    ) med on med.medid = e.detail3 and e.touchpoint1 in ('viewed_product', 'added_product')
    left join (
        select
        account_id,
        first_telemed_subs_order_timestamp
        from dwh.dim_user
    ) dim_user on dim_user.account_id = e.personid
    where
        session_start_time >= '2019-1-1'
        and ip_category = 'visitor'
    )
SELECT
	DATE(CONVERT_TIMEZONE('UTC', 'America/New_York', event_d2c.session_start_time )) AS "event_d2c.session_start_time_date",
	COUNT(DISTINCT case
        when (type_main = 'page' and (detail3 ilike '%mdv%' or touchpoint1 = 'drug'))
         or touchpoint1 = 'viewed_product' and subscription_end_date is null then global_session_id
      end
    ) AS "event_d2c.view_events_nonsubs",
	COUNT(DISTINCT case
        when touchpoint1 = 'added_product' and NVL(atc_is_subs,'f') = 'f' then global_session_id
      end
    ) AS "event_d2c.added_product_nonsubs",
	COUNT(DISTINCT case
        when touchpoint1 = 'created_account' and path != '/subs/checkout/1' then global_session_id
      end
    ) AS "event_d2c.created_account_nonsubs",
	COUNT(DISTINCT case
        when touchpoint1 = 'purchased_product' and not NVL(script_is_subs,FALSE) then global_session_id
      end
    ) AS "event_d2c.purchased_nonsubs_product",
	COUNT(DISTINCT case
        when touchpoint1 = 'filled_product' and not NVL(script_is_subs,FALSE) then global_session_id
      end
    ) AS "event_d2c.filled_nonsubs_product"
FROM event_d2c

WHERE ((((event_d2c.session_start_time ) >= ((CONVERT_TIMEZONE('America/New_York', 'UTC', DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()))) )))) AND (event_d2c.session_start_time ) < ((CONVERT_TIMEZONE('America/New_York', 'UTC', DATEADD(week,2, DATEADD(week,-1, DATE_TRUNC('week', DATE_TRUNC('day',CONVERT_TIMEZONE('UTC', 'America/New_York', GETDATE()))) ) ))))))) AND (NOT COALESCE(event_d2c.purchasing_patient , FALSE))
GROUP BY 1
ORDER BY 1 DESC
LIMIT 500