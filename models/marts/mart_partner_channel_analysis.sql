{{ config(materialized='table') }}

with territories as (
    select * from {{ ref('stg_territory') }}
),

resellers as (
    select * from {{ ref('stg_reseller') }}
),

products as (
    select * from {{ ref('stg_product_performance') }}
),

-- Create territory-reseller mapping (assume resellers operate in territories based on country)
territory_reseller_mapping as (
    select 
        t.*,
        r.*
    from territories t
    left join resellers r 
        on t.country_name_clean = r.country_region_clean
),

-- Calculate channel performance metrics
channel_analysis as (
    select
        -- Territory dimensions
        t.territory_key,
        t.region_name_clean as region,
        t.country_name_clean as country,
        t.region_group_clean as region_group,
        
        -- Territory classifications
        case 
            when t.country_name_clean = 'United States' then 'Domestic Market'
            when t.region_group_clean = 'North America' then 'NAFTA Region'
            when t.region_group_clean = 'Europe' then 'European Union'
            when t.region_group_clean = 'Pacific' then 'Asia Pacific'
            else 'Emerging Market'
        end as market_classification,
        
        -- Reseller dimensions
        r.reseller_id,
        r.reseller_name_clean as reseller_name,
        r.business_type_clean as business_type,
        r.city_clean as reseller_city,
        r.state_province_clean as reseller_state,
        
        -- Reseller classifications
        case 
            when upper(r.business_type_clean) like '%SPECIALTY%' then 'Specialty Retail'
            when upper(r.business_type_clean) like '%WAREHOUSE%' then 'Volume Retail'
            when upper(r.business_type_clean) like '%VALUE%' then 'Value Retail'
            else 'General Retail'
        end as retail_category,
        
        case 
            when r.country_region_clean = 'United States' then 'Domestic Partner'
            when r.country_region_clean in ('Canada', 'Mexico') then 'NAFTA Partner'
            else 'International Partner'
        end as geographic_tier,
        
        case 
            when r.state_province_clean in ('California', 'Texas', 'New York', 'Florida') then 'Major Market'
            when r.state_province_clean in ('Illinois', 'Pennsylvania', 'Ohio', 'Georgia') then 'Secondary Market'
            when r.country_region_clean = 'United States' then 'Tertiary Market'
            else 'International Market'
        end as market_classification_detail,
        
        -- Product portfolio suitability analysis
        count(distinct p.product_id) as total_products_in_territory,
        count(distinct p.product_category) as total_categories_in_territory,
        
        -- Product-Channel fit analysis
        count(distinct case 
            when upper(r.business_type_clean) like '%SPECIALTY%' and p.price_tier = 'Premium' 
            then p.product_id 
        end) as specialty_premium_products,
        
        count(distinct case 
            when upper(r.business_type_clean) like '%WAREHOUSE%' and p.price_tier in ('Standard', 'Budget') 
            then p.product_id 
        end) as warehouse_volume_products,
        
        count(distinct case 
            when upper(r.business_type_clean) like '%VALUE%' and p.price_tier = 'Budget' 
            then p.product_id 
        end) as value_budget_products,
        
        -- Category distribution
        count(distinct case when upper(p.product_group) = 'BIKES' then p.product_id end) as bikes_available,
        count(distinct case when upper(p.product_group) = 'COMPONENTS' then p.product_id end) as components_available,
        count(distinct case when upper(p.product_group) = 'APPAREL' then p.product_id end) as apparel_available,
        count(distinct case when upper(p.product_group) = 'ACCESSORIES' then p.product_id end) as accessories_available,
        
        -- Price tier distribution
        count(distinct case when p.price_tier = 'Premium' then p.product_id end) as premium_products,
        count(distinct case when p.price_tier = 'Mid-Range' then p.product_id end) as midrange_products,
        count(distinct case when p.price_tier = 'Standard' then p.product_id end) as standard_products,
        count(distinct case when p.price_tier = 'Budget' then p.product_id end) as budget_products,
        
        -- Financial metrics
        avg(p.standard_cost) as avg_product_cost_in_territory,
        sum(p.standard_cost) as total_inventory_value_potential,
        min(p.standard_cost) as lowest_price_point,
        max(p.standard_cost) as highest_price_point,
        
        -- Channel strategy scores (based on standard cost distribution)
        case 
            when upper(r.business_type_clean) like '%SPECIALTY%' then
                round(avg(case when p.price_tier = 'Premium' then p.standard_cost else 0 end) / 100, 1)
            when upper(r.business_type_clean) like '%WAREHOUSE%' then
                round(avg(case when p.price_tier in ('Standard', 'Budget') then p.standard_cost else 0 end) / 50, 1)
            else round(avg(p.standard_cost) / 75, 1)
        end as channel_product_fit_score,
        
        -- Market penetration potential
        case 
            when count(distinct p.product_id) >= 100 then 'High Potential'
            when count(distinct p.product_id) >= 50 then 'Medium Potential'
            else 'Limited Potential'
        end as market_penetration_potential,
        
        -- Channel strategy recommendation
        case 
            when upper(r.business_type_clean) like '%SPECIALTY%' and 
                 count(distinct case when p.price_tier = 'Premium' then p.product_id end) >= 20 then
                'Focus on Premium Portfolio'
            when upper(r.business_type_clean) like '%WAREHOUSE%' and 
                 count(distinct case when p.price_tier in ('Standard', 'Budget') then p.product_id end) >= 30 then
                'Focus on Volume Products'
            when upper(r.business_type_clean) like '%VALUE%' and 
                 count(distinct case when p.price_tier = 'Budget' then p.product_id end) >= 15 then
                'Focus on Value Products'
            else 'Diversified Portfolio Approach'
        end as recommended_strategy,
        
        -- Territory competitiveness score (0-100) - updated without cost_percentile
        round(
            (count(distinct p.product_id) * 0.3 +  -- Product availability (increased weight)
             avg(p.standard_cost) / 50 * 0.4 +     -- Product value indicator (new weight)
             case when t.country_name_clean = 'United States' then 30 else 20 end * 0.3), -- Market maturity
            1
        ) as territory_competitiveness_score,
        
        -- Audit fields
        current_timestamp() as dbt_updated_at
        
    from territory_reseller_mapping t
    left join resellers r on t.reseller_id = r.reseller_id
    cross join products p
    
    where r.reseller_id is not null  -- Only include territories with resellers
    
    group by 
        t.territory_key, t.region_name_clean, t.country_name_clean, t.region_group_clean,
        r.reseller_id, r.reseller_name_clean, r.business_type_clean, 
        r.city_clean, r.state_province_clean, r.country_region_clean
)

select * from channel_analysis