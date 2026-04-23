-- Granular sub-segmentation within each channel_ref bucket.
-- Replaces [segment_referral_new] Periscope macro.

{% macro segment_referral(channel_ref, utm_source, source_new, detail_source, referral_ts) %}

    case
        -- Referral sub-segments
        when {{ channel_ref }} = 'Referrals' then
            case
                when {{ referral_ts }} is not null                      then 'Referrals-V2'
                when {{ detail_source }} in ('referral-v2', 'LA_REF')  then 'Referrals-V2'
                when {{ utm_source }} = 'Sales-Referral'               then 'Sales-Referral'
                when {{ utm_source }} = 'Micro-Influencer'             then 'Micro-Influencer'
                when {{ utm_source }} = 'Employee-referral'            then 'Employee-referral'
                when {{ source_new }} = 'Teacher-Referral'
                    and {{ utm_source }} is null                        then 'Teacher-Referral'
                when {{ utm_source }} in (
                    'REFERRAL', 'Campaign_referral', 'referral'
                )                                                       then 'Parent-Referral'
                when {{ source_new }} = 'Parent-Referral'              then 'Parent-Referral'
                else                                                        'Teacher-Referral'
            end

        -- Organic sub-segments
        when {{ channel_ref }} = 'Organic' then
            case
                when {{ source_new }} = 'wildfire'
                    or {{ utm_source }} = 'wildfire'                    then 'Wildfire'
                when {{ source_new }} = 'CHATBOT'                      then 'Chatbot'
                when {{ utm_source }} in ('email', 'sms', 'whatsapp')  then 'Marcomm'
                when {{ utm_source }} in (
                    'ce-messenger', 'ce-info', 'ce-instagram'
                )                                                       then 'CE channels'
                when {{ source_new }} in ('TEACHER', 'INTEL', 'ADMIN') then 'TEACHER'
                when {{ source_new }} = 'CUEMATHAPP'                   then 'App'
                when {{ utm_source }} = 'UAE_LOCAL'                    then 'UAE Local'
                when {{ utm_source }} is null                           then 'website_organic'
                else                                                        'website_organic'
            end

        -- Performance sub-segments
        when {{ channel_ref }} = 'Performance' then
            case
                when {{ utm_source }} in ('S_GDN', 'S_SEM')            then 'Supply'
                when {{ utm_source }} in (
                    'd_affiliate_m', 'dc_magixengage',
                    'dc_apex', 'dc_epsilon', 'appnext_int',
                    'cashlelo_int', 'playdigo_int'
                )                                                       then 'Affiliates'
                when {{ utm_source }} = 'Brainbout'                    then 'Brainbout'
                when {{ utm_source }} in (
                    'SEM', 'Facebook', 'd_fb_m', 'd_pmax_m',
                    'quora', 'tiktok'
                )                                                       then 'Biddables'
                when {{ utm_source }} = 'googleadwords_int'            then 'App Biddables'
                when {{ utm_source }} = 'Facebook Ads'                 then 'App Biddables'
                when {{ utm_source }} = 'doubleclick_int'              then 'App Programmatic'
                else                                                        'Biddables'
            end

        -- Brand Partnership sub-segments
        when {{ channel_ref }} = 'Brand Partnership' then
            case
                when {{ utm_source }} in ('Gpay', 'COUPON', 'Paytm', 'Phonepe')
                                                                        then 'Scratch Cards'
                when {{ utm_source }} in ('IM', 'Influencer Marketing')
                                                                        then 'Influencer Marketing'
                else                                                        'Brand Partnership'
            end

        else {{ channel_ref }}
    end

{% endmacro %}
