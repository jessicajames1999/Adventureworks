-- {{ config(materialized='view') }}

-- with source_data as (
--     select
--         -- Primary key
--         ResellerKey as reseller_id,
        
--         -- Clean business information
--         trim(initcap(Reseller)) as reseller_name,
--         trim(`Business Type`) as business_type,
        
--         -- Clean location data
--         trim(initcap(City)) as city,
--         trim(`State-Province`) as state_province,
--         trim(`Country-Region`) as country_region,
        
--         -- Handle nulls and standardize
--         coalesce(trim(initcap(Reseller)), 'Unknown Reseller') as reseller_name_clean,
--         coalesce(trim(`Business Type`), 'Unknown Business Type') as business_type_clean,
--         coalesce(trim(initcap(City)), 'Unknown City') as city_clean,
--         coalesce(trim(`State-Province`), 'Unknown State') as state_province_clean,
--         coalesce(trim(`Country-Region`), 'Unknown Country') as country_region_clean,
        
--         -- Create location hierarchy
--         concat(
--             coalesce(trim(`Country-Region`), 'Unknown'),
--             ' | ',
--             coalesce(trim(`State-Province`), 'Unknown'),
--             ' | ',
--             coalesce(trim(City), 'Unknown')
--         ) as location_hierarchy,
        
--         -- Data quality flags
--         case 
--             when Reseller is null or trim(Reseller) = '' then true
--             else false
--         end as is_missing_reseller_name,
        
--         case 
--             when City is null or trim(City) = '' then true
--             else false
--         end as is_missing_city,
        
--         case 
--             when `Country-Region` is null or trim(`Country-Region`) = '' then true
--             else false
--         end as is_missing_country,
        
--         -- Audit fields
--         current_timestamp() as dbt_loaded_at,
--         'raw_adventureworks.dim_reseller' as source_table
        
--     from `chicory-mds.raw_adventureworks.dim_reseller`
--     where ResellerKey is not null
-- )

-- select * from source_data




{{ config(materialized='view') }}

with source_data as (
    select
        -- Primary key
        ResellerKey as reseller_id,
        
        -- Clean business information
        trim(initcap(Reseller)) as reseller_name,
        trim(`Business Type`) as business_type,
        
        -- Clean location data
        trim(initcap(City)) as city,
        trim(`State-Province`) as state_province,
        trim(`Country-Region`) as country_region,
        
        -- Handle nulls and standardize
        coalesce(trim(initcap(Reseller)), 'Unknown Reseller') as reseller_name_clean,
        coalesce(trim(`Business Type`), 'Unknown Business Type') as business_type_clean,
        coalesce(trim(initcap(City)), 'Unknown City') as city_clean,
        coalesce(trim(`State-Province`), 'Unknown State') as state_province_clean,
        coalesce(trim(`Country-Region`), 'Unknown Country') as country_region_clean,
        
        -- Create location hierarchy
        concat(
            coalesce(trim(`Country-Region`), 'Unknown'),
            ' | ',
            coalesce(trim(`State-Province`), 'Unknown'),
            ' | ',
            coalesce(trim(City), 'Unknown')
        ) as location_hierarchy,
        
        -- Data quality flags
        case 
            when Reseller is null or trim(Reseller) = '' then true
            else false
        end as is_missing_reseller_name,
        
        case 
            when City is null or trim(City) = '' then true
            else false
        end as is_missing_city,
        
        case 
            when `Country-Region` is null or trim(`Country-Region`) = '' then true
            else false
        end as is_missing_country,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'raw_adventureworks.dim_reseller' as source_table
        
    from {{ source('raw_adventureworks', 'dim_reseller') }}
    where ResellerKey is not null
)

select * from source_data