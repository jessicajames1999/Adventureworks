-- Test that all sales metrics are positive
select count(*) as failures
from {{ ref('mart_sales_performance_dashboard') }}
where bikes_specialized < 0 
   or components_specialized < 0 
   or apparel_specialized < 0
   or accessories_specialized < 0