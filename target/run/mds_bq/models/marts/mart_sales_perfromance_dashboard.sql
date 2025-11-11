
  
    

    create or replace table `chicory-mds`.`dbt_development_marts`.`mart_sales_perfromance_dashboard`
      
    
    

    
    OPTIONS()
    as (
      

with territories as (
    select * from `chicory-mds`.`dbt_development_staging`.`stg_territory`
),

salespeople as (
    select * from `chicory-mds`.`dbt_development_staging`.`stg_salesperson`
),

products as (
    select * from `chicory-mds`.`dbt_development_staging`.`stg_product_performance`
),

-- Create comprehensive sales performance metrics
sales_performance as (
    select
        -- Territory dimensions
        t.territory_key,
        t.region_name_clean as region,
        t.country_name_clean as country,
        t.region_group_clean as region_group,
        
        -- Territory business classifications
        case 
            when t.country_name_clean = 'United States' then 'Domestic'
            when t.region_group_clean = 'North America' then 'NAFTA'
            else 'International'
        end as market_type,
        
        case 
            when t.region_name_clean in ('Central', 'Northwest', 'Northeast') then 'High Priority'
            when t.region_name_clean in ('Southwest', 'Southeast') then 'Medium Priority'
            else 'Standard Priority'
        end as regional_priority,
        
        -- Salesperson dimensions
        s.employee_key,
        s.salesperson_name_clean as salesperson_name,
        s.job_title_clean as job_title,
        
        -- Salesperson classifications
        case 
            when upper(s.job_title_clean) like '%DIRECTOR%' then 'Executive'
            when upper(s.job_title_clean) like '%MANAGER%' then 'Management'
            when upper(s.job_title_clean) like '%REPRESENTATIVE%' then 'Individual Contributor'
            else 'Other'
        end as role_level,
        
        case 
            when upper(s.job_title_clean) like '%NORTH AMERICAN%' then 'Regional Coverage'
            when upper(s.job_title_clean) like '%DIRECTOR%' then 'Multi-Territory'
            when upper(s.job_title_clean) like '%MANAGER%' then 'Limited Territory'
            else 'Individual Territory'
        end as territory_scope,
        
        -- Product portfolio metrics (aggregated per territory/salesperson)
        count(distinct p.product_id) as total_products_available,
        count(distinct p.product_category) as total_categories_available,
        
        -- Product mix analysis
        count(distinct case when p.price_tier = 'Premium' then p.product_id end) as premium_products_count,
        count(distinct case when p.price_tier = 'Mid-Range' then p.product_id end) as midrange_products_count,
        count(distinct case when p.price_tier = 'Standard' then p.product_id end) as standard_products_count,
        count(distinct case when p.price_tier = 'Budget' then p.product_id end) as budget_products_count,
        
        -- Category breakdown
        count(distinct case when upper(p.product_group) = 'BIKES' then p.product_id end) as bikes_count,
        count(distinct case when upper(p.product_group) = 'COMPONENTS' then p.product_id end) as components_count,
        count(distinct case when upper(p.product_group) = 'APPAREL' then p.product_id end) as apparel_count,
        count(distinct case when upper(p.product_group) = 'ACCESSORIES' then p.product_id end) as accessories_count,
        
        -- Cost metrics
        avg(p.standard_cost) as avg_product_cost,
        min(p.standard_cost) as min_product_cost,
        max(p.standard_cost) as max_product_cost,
        sum(p.standard_cost) as total_inventory_value,
        
        -- Portfolio quality scores (based on average cost)
        round(avg(p.standard_cost), 1) as avg_standard_cost,
        
        -- Territory-Salesperson Performance Indicators
        case 
            when count(distinct p.product_id) >= 50 then 'High Product Diversity'
            when count(distinct p.product_id) >= 25 then 'Medium Product Diversity'
            else 'Low Product Diversity'
        end as product_diversity_level,
        
        case 
            when avg(p.standard_cost) >= 1000 then 'Premium Portfolio'
            when avg(p.standard_cost) >= 500 then 'Mid-Range Portfolio'
            else 'Value Portfolio'
        end as portfolio_positioning,
        
        -- Market opportunity score (0-100) - updated without cost_percentile
        round(
            (count(distinct p.product_id) * 0.4 +  -- Product diversity weight (increased)
             avg(p.standard_cost) / 50 * 0.3 +     -- Product value weight (new)
             case when t.country_name_clean = 'United States' then 30 else 20 end * 0.3), -- Market maturity weight
            1
        ) as market_opportunity_score,
        
        -- Audit fields
        current_timestamp() as dbt_updated_at
        
    from territories t
    cross join salespeople s  -- Assume all salespeople can sell in all territories
    cross join products p
    
    group by 
        t.territory_key, t.region_name_clean, t.country_name_clean, t.region_group_clean,
        s.employee_key, s.salesperson_name_clean, s.job_title_clean
)

select * from sales_performance
    );
  