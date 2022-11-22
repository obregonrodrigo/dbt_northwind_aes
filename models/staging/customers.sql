with identifies_duplicates as (
    select
        customer_id
        , company_name
        , contact_name
        , contact_title
        , address
        , city
        , region
        , postal_code
        , country
        , phone 
        -- verificação se o customer é duplicado, testando company e contact 
        , first_value(customer_id)
            over(partition by company_name, contact_name order by company_name
            rows between unbounded preceding and unbounded following) as result_duplicated

    from {{source('sources_northwind','customers')}}
), removed_duplicates as (
    -- de 94 foi pra 91 linhas
    select distinct result_duplicated from identifies_duplicates
), final as (
    select * from {{source('sources_northwind','customers')}} where customer_id in (select result_duplicated from removed_duplicates)
)

select *
from final