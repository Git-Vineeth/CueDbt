-- Fails if any row in mbt_leads has a null channel.
-- Every lead must be attributed — null channel means the macro logic has a gap.

select lead_id, channel_ref, utm_source
from {{ ref('mbt_leads') }}
where channel is null
