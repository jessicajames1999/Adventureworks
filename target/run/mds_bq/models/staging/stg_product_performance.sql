

  create or replace view `chicory-mds`.`dbt_development_staging`.`stg_product_performance`
  OPTIONS()
  as -- 

-- with source_data as (
--     select
--         -- Clean and standardize columns
--         product_id,
        
--         -- Clean product name: trim whitespace, title case
--         trim(initcap(product_name)) as product_name,
        
--         -- Standardize categories: uppercase, handle nulls
--         coalesce(upper(trim(product_category)), 'UNCATEGORIZED') as product_category,
--         coalesce(upper(trim(product_subcategory)), 'UNCATEGORIZED') as product_subcategory,
        
--         -- Clean color: title case, handle nulls
--         coalesce(initcap(trim(product_color)), 'Unknown') as product_color,
        
--         -- Ensure proper data types and handle edge cases
--         case 
--             when standard_cost < 0 then 0.00  -- Handle negative costs
--             when standard_cost is null then 0.00
--             else round(cast(standard_cost as float64), 2)
--         end as standard_cost,
        
--         -- Standardize price tier
--         coalesce(upper(trim(price_tier)), 'STANDARD') as price_tier,
        
--         -- Clean product group
--         coalesce(upper(trim(product_group)), 'GENERAL') as product_group,
        
--         -- Standardize color category
--         coalesce(upper(trim(color_category)), 'OTHER') as color_category,
        
        
--         -- Clean timestamp
--         cast(created_at as timestamp) as created_at,
        
--         -- Standardize source system
--         coalesce(upper(trim(source_system)), 'UNKNOWN') as source_system,
        
--         -- Add data quality flags
--         case 
--             when product_name is null or trim(product_name) = '' then true
--             else false
--         end as is_missing_product_name,
        
--         -- Add audit fields
--         current_timestamp() as dbt_loaded_at
        
--     from `mapper_enriched.product_performance_mapper`
    
--     -- Filter out obviously bad records (FIXED: only check for null, not empty string)
--     where product_id is not null
-- )

-- select * from source_data





with source_data as (
    select
        -- Clean and standardize columns
        product_id,
        
        -- Clean product name: trim whitespace, title case
        trim(initcap(product_name)) as product_name,
        
        -- Standardize categories: uppercase, handle nulls
        coalesce(upper(trim(product_category)), 'UNCATEGORIZED') as product_category,
        coalesce(upper(trim(product_subcategory)), 'UNCATEGORIZED') as product_subcategory,
        
        -- Clean color: title case, handle nulls
        coalesce(initcap(trim(product_color)), 'Unknown') as product_color,
        
        -- Ensure proper data types and handle edge cases
        case 
            when standard_cost < 0 then 0.00  -- Handle negative costs
            when standard_cost is null then 0.00
            else round(cast(standard_cost as float64), 2)
        end as standard_cost,
        
        -- Standardize price tier
        coalesce(upper(trim(price_tier)), 'STANDARD') as price_tier,
        
        -- Clean product group
        coalesce(upper(trim(product_group)), 'GENERAL') as product_group,
        
        -- Standardize color category
        coalesce(upper(trim(color_category)), 'OTHER') as color_category,
        
        -- Clean timestamp
        cast(created_at as timestamp) as created_at,
        
        -- Standardize source system
        coalesce(upper(trim(source_system)), 'UNKNOWN') as source_system,
        
        -- Add data quality flags
        case 
            when product_name is null or trim(product_name) = '' then true
            else false
        end as is_missing_product_name,
        
        -- Add audit fields
        current_timestamp() as dbt_loaded_at
        
    from `chicory-mds`.`mapper_enriched`.`product_performance_mapper`
    
    -- Filter out obviously bad records
    where product_id is not null
)

select * from source_data;

