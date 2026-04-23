-- Grain: one row per parent (signup profile).
-- Source: application_service_parent_student.parent_profile
-- Contains UTM params, device type, OS and IP city captured at the moment of signup.
-- This is the fallback source for landing page and city when user_source_log has no data.

with source as (

    select * from {{ source('app', 'parent_profile') }}

),

renamed as (

    select
        parent_id,
        meta.utm_params."landing_page"::text                   as landing_page,
        meta.headers."cf-ipcity"::text                         as ip_city,
        meta.user_device."device"::text                        as device_type,
        meta.user_device."operatingSystem"::text               as os_type,

        -- Landing page category at signup (fallback when session page is missing)
        {{ landingpage_grouping('meta.utm_params."landing_page"::text') }}
                                                               as landing_type

    from source

)

select * from renamed
