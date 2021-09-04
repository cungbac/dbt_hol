select book
    , date
    , trader
    , instrument
    , action
    , cost
    , currency
    , volume
    , cost_per_share
    , stock_exchange_name
    , sum(t.volume) over ( partition by 
                            t.instrument 
                            , t.stock_exchange_name
                            , trader
                        order by t.date
                        rows unbounded preceding
                        ) total_shares
from {{ref('tfm_book')}} t

union all

select book
    , date
    , trader
    , instrument
    , 'HOLD' as action
    , 0 as cost
    , currency
    , 0 as volume
    , 0 as cost_per_share
    , stock_exchange_name
    , total_shares
from {{ref('tfm_daily_position')}}
where (date, trader, instrument, book, stock_exchange_name)
    not in
        (select date, trader, instrument, book, stock_exchange_name
            from {{ref('tfm_book')}}
        )
