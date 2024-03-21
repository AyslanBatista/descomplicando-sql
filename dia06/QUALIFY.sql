-- Databricks notebook source
SELECT
  idProduto,
  descCategoria,
  vlPesoGramas,
  row_number() OVER (
    PARTITION BY descCategoria
    ORDER BY
      vlPesoGramas DESC
  ) AS rnProduto
FROM
  silver.olist.produto
WHERE
  descCategoria IS NOT NULL 
  
  
QUALIFY rnProduto = 1

-- COMMAND ----------

SELECT
  *,
  vlComprimentoCm * vlAlturaCm * vlLarguraCm AS volumeCm3
FROM
  silver.olist.produto
WHERE
  descCategoria IS NOT NULL QUALIFY row_number() OVER (
    PARTITION BY descCategoria
    ORDER BY
      vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC
  ) < = 5

-- COMMAND ----------

SELECT *,
row_number() OVER (PARTITION BY descCategoria ORDER BY vlPesoGramas DESC) AS rnPeso,
row_number() OVER (PARTITION BY descCategoria ORDER BY vlComprimentoCm * vlAlturaCm * vlLarguraCm DESC) AS rnVolume

FROM silver.olist.produto

WHERE descCategoria IS NOT NULL 

QUALIFY rnPeso <= 5 AND rnVolume <= 5
