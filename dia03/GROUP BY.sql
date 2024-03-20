-- Databricks notebook source
SELECT
  descUF,
  count(DISTINCT idVendedor)
FROM
  silver.olist.vendedor
GROUP BY
  descUF
ORDER BY
  descUF

-- COMMAND ----------

SELECT
  descUF,
  count(DISTINCT idVendedor)
FROM
  silver.olist.vendedor
GROUP BY
  descUF
ORDER BY
  count(DISTINCT idVendedor)

-- COMMAND ----------

SELECT
  descUF,
  count(DISTINCT idVendedor) AS qtVendedor
FROM
  silver.olist.vendedor
GROUP BY
  descUF
ORDER BY
  qtVendedor DESC

-- COMMAND ----------

SELECT
  descCategoria,
  count(DISTINCT idProduto) as qtProduto,
  avg(vlPesoGramas) as avgPeso,
  percentile(vlComprimentoCm, 0.5) as medianaPeso,
  avg(vlComprimentoCm * vlAlturaCm * vlLarguraCm) as avgVolume,
  percentile(vlComprimentoCm * vlAlturaCm * vlLarguraCm, 0.5) as medianaVolume
FROM
  silver.olist.produto
GROUP BY
  descCategoria
ORDER BY
  qtProduto DESC

-- COMMAND ----------

SELECT
  year(dtPedido),
  count(idPedido)
FROM
  silver.olist.pedido
GROUP BY
  year(dtPedido)

-- COMMAND ----------

SELECT
  year(dtPedido) || '-' || month(dtPedido),
  AS anoMes count(idPedido)
FROM
  silver.olist.pedido
GROUP BY
  anoMes
ORDER BY
  anoMes

-- COMMAND ----------

SELECT
  date(date_trunc('month', dtPedido)) as anoMes,
FROM
  silver.olist.pedido
GROUP BY
  anoMes
ORDER BY
  anoMes
