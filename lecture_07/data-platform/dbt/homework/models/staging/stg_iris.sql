{{ config(materialized='view') }}

SELECT
    sepal_length,
    sepal_width,
    petal_length,
    petal_width,
    species
FROM {{ ref('iris_dataset') }}
