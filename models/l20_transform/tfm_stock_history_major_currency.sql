-- SELECT tsh.*
--      , fx_gbp.value * tsh."Open"    AS gbp_open      
--      , fx_gbp.value * tsh."High"    AS gbp_high		
--      , fx_gbp.value * tsh."Low"     AS gbp_low      
--      , fx_gbp.value * tsh."Close"   AS gbp_close    
--      , fx_eur.value * tsh."Open"    AS eur_open      
--      , fx_eur.value * tsh."High"    AS eur_high		
--      , fx_eur.value * tsh."Low"     AS eur_low      
--      , fx_eur.value * tsh."Close"   AS eur_close    
--   FROM {{ref('tfm_stock_history')}} tsh
--      , {{ref('tfm_fx_rates')}}      fx_gbp
--      , {{ref('tfm_fx_rates')}}      fx_eur
--  WHERE fx_gbp.currency              = 'USD/GBP'     
--    AND fx_eur.currency              = 'USD/EUR'     
--    AND tsh.date                     = fx_gbp.date
--    AND tsh.date                     = fx_eur.date

-- 10302971 -- 10302971
SELECT tsh.*
     , fx_gbp.value * tsh."Open"    AS gbp_open      
     , fx_gbp.value * tsh."High"    AS gbp_high		
     , fx_gbp.value * tsh."Low"     AS gbp_low      
     , fx_gbp.value * tsh."Close"   AS gbp_close    
     , fx_eur.value * tsh."Open"    AS eur_open      
     , fx_eur.value * tsh."High"    AS eur_high		
     , fx_eur.value * tsh."Low"     AS eur_low      
     , fx_eur.value * tsh."Close"   AS eur_close    
FROM {{ref('tfm_stock_history')}} tsh
JOIN {{ref('tfm_fx_rates')}}      fx_gbp
    ON tsh.date = fx_gbp.date
JOIN {{ref('tfm_fx_rates')}}      fx_eur
    ON tsh.date = fx_eur.date
WHERE   fx_gbp.currency     = 'USD/GBP'     
   AND  fx_eur.currency     = 'USD/EUR'     