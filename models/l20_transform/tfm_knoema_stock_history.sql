-- Using snowflake pivot --------
-- with cts as
-- (
--     select 
--         company_symbol
--         , company_name
--         , stock_exchange_name
--         , indicator_name
--         , date
--         , value
--         , data_source_name
--     from {{ref('base_knoema_stock_history')}} src
--     where indicator_name in ('Close', 'Open','High','Low', 'Volume', 'Change %')
-- )

-- select *
-- from cts
-- pivot(
--     sum(value) for indicator_name in ('Close','Open','High','Low', 'Volume', 'Change %')
--     )
-- as p(company_symbol, company_name, stock_exchange_name, date, data_source_name, close ,open ,high,low,volume,change)  

----------------------------------------------------------
-- Using dbt_utils 35034427 ----------- 35034427
select 
    company_symbol
    ,company_name
    ,stock_exchange_name
    ,date
    ,data_source_name
    ,{{
        dbt_utils.pivot(
            column = 'indicator_name'
            , values = dbt_utils.get_column_values(
                ref('base_knoema_stock_history'),
                'indicator_name'
            )
            , then_value = 'value'
        )
    }}
from {{ref('base_knoema_stock_history')}}
group by
    company_symbol
    , company_name
    , stock_exchange_name
    , date
    , data_source_name