{{
    config(
        materialized = 'table',
        tags = ["Reference Data"]
    )
}}

select src.*
from {{ref('base_knoema_fx_rates')}} src
where 
    "Indicator Name"    = 'Close'
    and "Frequency"     = 'D'
    and "Date"          > '2016-01-01'