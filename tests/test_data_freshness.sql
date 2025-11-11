-- Test that data was updated recently (within 48 hours)
select count(*) as failures
from {{ ref('mart_sales_performance_dashboard') }}
where dbt_updated_at < current_timestamp() - interval 48 hour