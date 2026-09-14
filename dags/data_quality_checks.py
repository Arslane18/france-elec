_SILVER_DATA_QUALITY_CHECKS = [
    (
        "duplicates",
        """
        SELECT DATE_HEURE, REGION_CODE, COUNT(*) AS nb
        FROM SILVER.CONSO_METEO_HORAIRE
        GROUP BY 1, 2
        HAVING COUNT(*) > 1
        """,
    ),
    (
        "gaps",
        """
        SELECT REGION_CODE, DATE_HEURE,
               DATEDIFF('hour', LAG(DATE_HEURE) OVER (PARTITION BY REGION_CODE ORDER BY DATE_HEURE), DATE_HEURE) AS ecart_h
        FROM SILVER.CONSO_METEO_HORAIRE
        QUALIFY ecart_h > 2
        """,
    ),
    (
        "freshness",
        """
        SELECT REGION_CODE, MAX(DATE_HEURE) AS derniere_donnee
        FROM SILVER.CONSO_METEO_HORAIRE
        GROUP BY REGION_CODE
        HAVING DATEDIFF('hour', derniere_donnee, CURRENT_TIMESTAMP()) > 48
        """,
    ),
    (
        "outliers",
        """
        SELECT DATE_HEURE, REGION_CODE, CONSOMMATION
        FROM SILVER.CONSO_METEO_HORAIRE
        WHERE CONSOMMATION IS NULL OR CONSOMMATION < 0 OR CONSOMMATION > 50000
        """,
    ),
]