{% snapshot lsq_lead_snapshot %}

    {{
        config(
            target_schema='snapshots',
            unique_key='lead_id',
            strategy='check',
            check_cols=['stage', 'channel_ref', 'qualified_bucket', 'enrolled', 'cna_completed', 'pac_completed']
        )
    }}

    -- Captures historical CRM stage changes per lead.
    -- Allows point-in-time queries: "what was a lead's stage on date X?"
    -- dbt adds dbt_valid_from (when this state started) and dbt_valid_to (when it ended, NULL = current).
    -- Run with: dbt snapshot

    select * from {{ ref('stg_lsq__lead_data') }}

{% endsnapshot %}
