select 
    "Currency" currency,
    "Currency Unit" currency_unit,
    "Frequency" frequency,
    "Date" date,
    "Value" value,
    'Knoem.FX Rates' data_source_name,
    src.*
from {{source('knoema_economy_data_atlas','exratescc2018')}} src