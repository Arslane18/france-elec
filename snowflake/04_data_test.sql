-- to automate when I have time
USE DATABASE ELEC_FORECAST;

-- test to detect duplicates
SELECT DATE_HEURE, REGION_CODE, COUNT(*) AS nb
FROM SILVER.CONSO_METEO_HORAIRE
GROUP BY 1, 2
HAVING COUNT(*) > 1;

-- no missing values (check if there is a difference of more of 2 hours between 2 consecutive hours)
SELECT REGION_CODE, DATE_HEURE,
       DATEDIFF('hour', LAG(DATE_HEURE) OVER (PARTITION BY REGION_CODE ORDER BY DATE_HEURE), DATE_HEURE) AS ecart_h
FROM SILVER.CONSO_METEO_HORAIRE
QUALIFY ecart_h > 2;


-- TODO
-- data freshness idk 
-- check if data are correct (like no outliers like 1M kw of consumption in a day)