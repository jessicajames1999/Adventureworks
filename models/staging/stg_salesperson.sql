-- {{ config(materialized='view') }}

-- with source_data as (
--     select
--         -- Primary keys
--         cast(EmployeeKey as int64) as employee_key,
--         cast(EmployeeID as string) as employee_id,  -- Convert to string first
        
--         -- Clean personal information
--         trim(initcap(Salesperson)) as salesperson_name,
--         trim(Title) as job_title,
--         lower(trim(UPN)) as email_address,
        
--         -- Handle nulls and empty strings
--         case 
--             when trim(Salesperson) = '' or Salesperson is null then 'Unknown Salesperson'
--             else trim(initcap(Salesperson))
--         end as salesperson_name_clean,
        
--         case 
--             when trim(Title) = '' or Title is null then 'Unknown Title'
--             else trim(Title)
--         end as job_title_clean,
        
--         case 
--             when trim(UPN) = '' or UPN is null then 'unknown@company.com'
--             else lower(trim(UPN))
--         end as email_address_clean,
        
--         -- Extract email domain
--         case 
--             when UPN like '%@%' then regexp_extract(lower(trim(UPN)), r'@(.+)')
--             else 'unknown.com'
--         end as email_domain,
        
--         -- Data quality flags
--         case 
--             when Salesperson is null or trim(Salesperson) = '' then true
--             else false
--         end as is_missing_name,
        
--         case 
--             when UPN is null or trim(UPN) = '' or UPN not like '%@%' then true
--             else false
--         end as is_invalid_email,
        
--         -- Audit fields
--         current_timestamp() as dbt_loaded_at,
--         'raw_adventureworks.dim_salesperson' as source_table
        
--     from `chicory-mds.raw_adventureworks.dim_salesperson`
--     where EmployeeKey is not null
-- )

-- select * from source_data


{{ config(materialized='view') }}

with source_data as (
    select
        -- Primary keys
        cast(EmployeeKey as int64) as employee_key,
        cast(EmployeeID as string) as employee_id,
        
        -- Clean personal information
        trim(initcap(Salesperson)) as salesperson_name,
        trim(Title) as job_title,
        lower(trim(UPN)) as email_address,
        
        -- Handle nulls and empty strings
        case 
            when trim(Salesperson) = '' or Salesperson is null then 'Unknown Salesperson'
            else trim(initcap(Salesperson))
        end as salesperson_name_clean,
        
        case 
            when trim(Title) = '' or Title is null then 'Unknown Title'
            else trim(Title)
        end as job_title_clean,
        
        case 
            when trim(UPN) = '' or UPN is null then 'unknown@company.com'
            else lower(trim(UPN))
        end as email_address_clean,
        
        -- Extract email domain
        case 
            when UPN like '%@%' then regexp_extract(lower(trim(UPN)), r'@(.+)')
            else 'unknown.com'
        end as email_domain,
        
        -- Data quality flags
        case 
            when Salesperson is null or trim(Salesperson) = '' then true
            else false
        end as is_missing_name,
        
        case 
            when UPN is null or trim(UPN) = '' or UPN not like '%@%' then true
            else false
        end as is_invalid_email,
        
        -- Audit fields
        current_timestamp() as dbt_loaded_at,
        'raw_adventureworks.dim_salesperson' as source_table
        
    from {{ source('raw_adventureworks', 'dim_salesperson') }}
    where EmployeeKey is not null
)

select * from source_data
