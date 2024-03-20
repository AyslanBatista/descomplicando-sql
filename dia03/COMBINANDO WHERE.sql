-- Databricks notebook source
SELECT
  count(idVendedor),
  count(DISTINCT idVendedor)
FROM
  silver.olist.vendedor
WHERE
  descUF = 'RJ'

-- COMMAND ----------

SELECT
  count(DISTINCT idCliente), --  clientes não únicos
  count(DISTINCT idClienteUnico), --  clientes únicos
  count(DISTINCT descCidade) -- cidades distintas
FROM
  silver.olist.cliente
WHERE
  descUF = 'SP'

-- COMMAND ----------

SELECT
  count(*),
  avg(vlPesoGramas),
  percentile(vlPesoGramas, 0.5),
  std(vlPesoGramas),
  min(vlPesoGramas),
  max(vlPesoGramas),
  max(vlPesoGramas) - min(vlPesoGramas)
FROM
  silver.olist.produto
WHERE
  descCategoria = 'bebes'
