{{ config(severity='warn') }}

-- Warns if any lead_id appears more than once in mbt_leads.
-- Duplicates originate in the LSQ source view — tracked as data quality issue, not dbt bug.

select lead_id, count(*) as row_count
from {{ ref('mbt_leads') }}
group by 1
having count(*) > 1
