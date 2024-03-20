-- Databricks notebook source
SELECT
  descCategoria,
  count(DISTINCT idProduto) AS qtdeProduto,
  avg(vlPesoGramas) AS avgPeso
FROM
  silver.olist.produto
WHERE
  descCategoria IN ('bebes', 'perfumaria', 'moveis_decoracao')
  OR descCategoria like '%moveis%'
GROUP BY
  descCategoria
HAVING
  count(DISTINCT idProduto) > 100
  AND avgPeso > 1000
ORDER BY
  avgPeso
