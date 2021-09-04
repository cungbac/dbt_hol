with cst_market_days as
(
    select distinct date
    from {{ref('tfm_stock_history_major_currency')}} hist
    where hist.date >= (select min(date) as min_dt from {{ref('tfm_book')}})
)

select 
    cst_market_days.date,
    trader,
    stock_exchange_name,
    instrument,
    book,
    currency,
    sum(volume) as total_shares
from cst_market_days
    , {{ref('tfm_book')}} book
where book.date <= cst_market_days.date
{{group_by(6)}}