-- Test that we have a reasonable number of rows
select case when count(*) between 10 and 1000 then 0 else 1 end as failures
from {{ ref('mart_sales_performance_dashboard') }}