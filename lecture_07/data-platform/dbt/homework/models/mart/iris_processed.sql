{{ config(materialized='table') }}

SELECT
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species,
    CASE
        WHEN species = 'setosa' THEN 1
        ELSE 0
    END AS species_setosa,
    CASE
        WHEN species = 'versicolor' THEN 1
        ELSE 0
    END AS species_versicolor,
    CASE
        WHEN species = 'virginica' THEN 1
        ELSE 0
    END AS species_virginica
FROM {{ ref('stg_iris') }}
