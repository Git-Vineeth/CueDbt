-- Fails if any lead_id appears more than once in mbt_leads.
-- Each lead must be unique — duplicates cause double-counting in all marketing metrics.

select lead_id, count(*) as row_count
from {{ ref('mbt_leads') }}
group by 1
having count(*) > 1
