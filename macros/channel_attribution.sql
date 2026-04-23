-- Top-level channel attribution — replaces [channel_test_referral_logic] Periscope macro.
-- Returns channel_ref: one of Referrals / Performance / Brand Partnership / Organic / Others.
-- Priority order is strict: Referrals always wins, Others always loses.

{% macro channel_attribution(utm_source, source_new, detail_source, referral_ts) %}

    case
        -- 1. Referrals (highest priority — any referral signal wins)
        when {{ referral_ts }} is not null                                      then 'Referrals'
        when {{ detail_source }} = 'referral-v2'                               then 'Referrals'
        when {{ utm_source }} in (
                'REFERRAL', 'Campaign_referral', 'referral',
                'Sales-Referral', 'Teacher-Referral', 'Parent-Referral',
                'Micro-Influencer', 'Employee-referral', 'LA_REF'
            )                                                                   then 'Referrals'
        when {{ utm_source }} is null
            and {{ source_new }} in ('Parent-Referral', 'Teacher-Referral')    then 'Referrals'

        -- 2. Performance (paid channels)
        when {{ utm_source }} in (
                'SEM', 'Facebook', 'AFFILIATE', 'quora',
                'd_fb_m', 'd_pmax_m', 'dc_epsilon',
                'googleadwords_int', 'Facebook Ads', 'tiktok',
                'doubleclick_int', 'appnext_int', 'cashlelo_int',
                'playdigo_int', 'dc_magixengage', 'd_affiliate_m',
                'dc_apex', 'S_GDN', 'S_SEM', 'Brainbout'
            )                                                                   then 'Performance'

        -- 3. Brand Partnership
        when {{ utm_source }} in (
                'Gpay', 'IM', 'Influencer Marketing', 'COUPON',
                'Paytm', 'Phonepe'
            )                                                                   then 'Brand Partnership'

        -- 4. Organic
        when {{ utm_source }} is null                                           then 'Organic'
        when {{ source_new }} in (
                'TEACHER', 'INTEL', 'ADMIN', 'CUEMATHAPP',
                'LEAP-ADMIN', 'wildfire', 'CHATBOT'
            )                                                                   then 'Organic'
        when {{ utm_source }} in (
                'email', 'sms', 'whatsapp', 'ce-messenger',
                'ce-info', 'ce-instagram', 'UAE_LOCAL'
            )                                                                   then 'Organic'

        else 'Others'
    end

{% endmacro %}
