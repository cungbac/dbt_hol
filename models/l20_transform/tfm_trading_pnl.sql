select
    t.instrument
    , t.stock_exchange_name
    , t.date
    , trader
    , t.volume
    , t.cost
    , cost_per_share
    , currency
    , sum(cost) over(partition by 
                        t.instrument
                        , t.stock_exchange_name
                        , trader
                    order by t.date
                    rows unbounded preceding) as cash_cumulative
    ,   case
            when t.currency = 'GBP' then gbp_close
            when t.currency = 'EUR' then eur_close
            else s."Close"
        end as close_price_matching_ccy
    , total_shares * close_price_matching_ccy as market_value
    , total_shares * close_price_matching_ccy + cash_cumulative as PnL
from        {{ref('tfm_daily_position_with_trades')}} t
inner join  {{ref('tfm_stock_history_major_currency')}} s
    on t.instrument = s.company_symbol
    and s.date = t.date
    and t.stock_exchange_name = s.stock_exchange_name