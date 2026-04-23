-- Grain: one row per student (latest tracking event).
-- Source: application_service_parent_student.user_source_log
-- Deduplication: latest event per student (ROW_NUMBER on created_on DESC).
-- Provides landing page, IP city, browser and OS from Cloudflare headers.
-- Use event_ts_ist for date grouping — data is already in IST.

with source as (

    select * from {{ source('app', 'user_source_log') }}
    where user_type = 'STUDENT'

),

deduped as (

    select
        *,
        row_number() over (
            partition by user_id
            order by created_on desc
        ) as row_num

    from source

),

latest as (

    select * from deduped where row_num = 1

),

renamed as (

    select
        user_id                                                         as student_service_id,
        meta."landing_page_24_hours"."page_profile"::text              as last_24hr_landing_page,
        meta."landing_page_session"."page_profile"::text               as session_landing_page,
        meta."cf-ipcity"::text                                         as ip_city,
        meta.browser_family::text                                      as browser,
        meta.os_family::text                                           as os,
        created_on                                                     as tracked_at

    from latest

)

select * from renamed
