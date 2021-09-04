{{
    config(
        materialized = 'incremental',
        tags = ['Fact Data']
    )
}}

select src.*
from {{ref('tfm_trading_pnl')}} src

{% if is_incremental() %}

    where (trader, instrument, date, stock_exchange_name)
        not in
            (select trader, instrument, date, stock_exchange_name from {{this}})

{% endif %}