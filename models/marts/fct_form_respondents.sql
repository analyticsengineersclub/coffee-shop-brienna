{{ 
    config(
        materialized='incremental',
        unique_key='github_username' -- prevent duplicates
    ) 
}}

with events as (
    select * from {{ ref('stg_form_events') }}

    {% if is_incremental() %}
    where github_username in ( -- ensures that we find the last event associated with the respondent
        select distinct github_username from {{ source('advanced_dbt_examples', 'form_events') }}
        where timestamp >= ( 
            select 
                max(last_form_entry) 
            from {{ this }} 
            -- we don't want to use a ref, as this creates a cyclic dependency
            -- we also don't want to hard code it, e.g. dbt_brienna.fct_form_respondents, 
            -- cuz then it wouldn't work if someone else is using the model
        )
    )
    
    {% endif %}
),

aggregated as (
    select
        github_username,
        min(timestamp) as first_form_entry,
        max(timestamp) as last_form_entry,
        count(*) as number_of_entries
    from events
    group by 1
)

select * from aggregated