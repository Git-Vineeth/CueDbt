-- Normalises UTM medium into 7 buckets.
-- Replaces the inline utm_medium CASE blocks scattered across mbt_leads and mbt_payments.

{% macro utm_medium_grouping(utm_medium_col, utm_source_col) %}

    case
        when (
                {{ utm_medium_col }} ilike '%Go_%'
                or {{ utm_medium_col }} ilike '%go_%'
                or {{ utm_medium_col }} ilike '%Sem_Search%'
            )
            and {{ utm_source_col }} ilike '%brand%'
            then 'google_brand'

        when {{ utm_medium_col }} ilike '%Go_%'
            or {{ utm_medium_col }} ilike '%go_%'
            or {{ utm_medium_col }} ilike '%Sem_Search%'
            then 'google_other'

        when {{ utm_medium_col }} ilike '%fb%'
            or {{ utm_medium_col }} ilike '%FB%'
            then 'meta'

        when {{ utm_medium_col }} ilike '%AD%'
            or {{ utm_medium_col }} ilike '%dc_whatsappflow%'
            then 'whatsapp'

        when {{ utm_medium_col }} ilike '%BTL%'
            or {{ utm_medium_col }} ilike '%offline_community%'
            then 'BTL'

        when {{ utm_medium_col }} ilike '%influencer%'
            then 'Influencer'

        else 'others'
    end

{% endmacro %}
