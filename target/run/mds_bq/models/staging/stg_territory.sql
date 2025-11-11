

  create or replace view `chicory-mds`.`dbt_development_staging`.`stg_territory`
  OPTIONS()
  as -- 

-- with source_data as (
--     select
--         -- Primary key
--         cast(SalesTerritoryKey as int64) as territory_key,
        
--         -- Clean territory information
--         trim(Region) as region_name,
--         trim(Country) as country_name,
--         trim(`Group`) as region_group,
        
--         -- Standardize nulls and empty strings
--         case 
--             when trim(Region) = '' or Region is null then 'Unknown Region'
--             else trim(Region)
--         end as region_name_clean,
        
--         case 
--             when trim(Country) = '' or Country is null then 'Unknown Country'
--             else trim(Country)
--         end as country_name_clean,
        
--         case 
--             when trim(`Group`) = '' or `Group` is null then 'Unknown Group'
--             else trim(`Group`)
--         end as region_group_clean,
        
--         -- Add audit fields
--         current_timestamp() as dbt_loaded_at,
--         'raw_adventureworks.dim_region' as source_table
        
--     from `chicory-mds.raw_adventureworks.dim_region`
--     where SalesTerritoryKey is not null
-- )

-- select * from source_data




with source_data as (
    select
        -- Primary key
        cast(SalesTerritoryKey as int64) as territory_key,
        
        -- Clean territory information
        trim(Region) as region_name,
        trim(Country) as country_name,
        trim(`Group`) as region_group,
        
        -- Standardize nulls and empty strings
        case 
            when trim(Region) = '' or Region is null then 'Unknown Region'
            else trim(Region)
        end as region_name_clean,
        
        case 
            when trim(Country) = '' or Country is null then 'Unknown Country'
            else trim(Country)
        end as country_name_clean,
        
        case 
            when trim(`Group`) = '' or `Group` is null then 'Unknown Group'
            else trim(`Group`)
        end as region_group_clean,
        
        -- Add audit fields
        current_timestamp() as dbt_loaded_at,
        'raw_adventureworks.dim_region' as source_table
        
    from `chicory-mds`.`raw_adventureworks`.`dim_region`
    where SalesTerritoryKey is not null
)

select * from source_data;

