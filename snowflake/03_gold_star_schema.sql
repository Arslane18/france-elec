USE DATABASE ELEC_FORECAST;
USE SCHEMA GOLD;

CREATE TABLE IF NOT EXISTS DIM_DATE (
    DATE_KEY        DATE PRIMARY KEY,
    ANNEE           INT,
    MOIS            INT,
    JOUR            INT,
    JOUR_SEMAINE    VARCHAR(10)
);

INSERT INTO DIM_DATE (DATE_KEY, ANNEE, MOIS, JOUR, JOUR_SEMAINE)
SELECT 
    DATEADD(day, seq4(), '2013-01-01') AS DATE_KEY,
    YEAR(DATE_KEY)                     AS ANNEE,
    MONTH(DATE_KEY)                    AS MOIS,
    DAY(DATE_KEY)                      AS JOUR,
    DAYNAME(DATE_KEY)                  AS JOUR_SEMAINE
FROM TABLE(GENERATOR(ROWCOUNT => 7000)) -- roughly 19 years
ORDER BY DATE_KEY;


CREATE TABLE IF NOT EXISTS DIM_REGION (
    REGION_CODE   NUMBER PRIMARY KEY,
    REGION_NOM    VARCHAR(100)
);

INSERT INTO DIM_REGION (REGION_CODE, REGION_NOM) VALUES
    (11, 'Île-de-France'),
    (24, 'Centre-Val de Loire'),
    (27, 'Bourgogne-Franche-Comté'),
    (28, 'Normandie'),
    (32, 'Hauts-de-France'),
    (44, 'Grand Est'),
    (52, 'Pays de la Loire'),
    (53, 'Bretagne'),
    (75, 'Nouvelle-Aquitaine'),
    (76, 'Occitanie'),
    (84, 'Auvergne-Rhône-Alpes'),
    (93, 'Provence-Alpes-Côte d''Azur');

CREATE TABLE IF NOT EXISTS FACT_CONSOMMATION_HORAIRE (
    DATE_HEURE                TIMESTAMP_NTZ NOT NULL,
    DATE_KEY                  DATE          NOT NULL REFERENCES DIM_DATE(DATE_KEY),
    REGION_CODE               NUMBER        NOT NULL REFERENCES DIM_REGION(REGION_CODE),
    CONSOMMATION              FLOAT,
    TEMPERATURE_2M            FLOAT,
    PRECIPITATION             FLOAT,
    EST_JOUR_FERIE            INT           DEFAULT 0,
    CONSO_MOYENNE_MOBILE_7J   FLOAT,
    CONSO_J_MOINS_7           FLOAT,
    PRIMARY KEY (DATE_HEURE, REGION_CODE)
);

