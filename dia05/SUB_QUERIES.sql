-- Databricks notebook source
-- 1. Quais são os TOP 10 vendedores que mais venderam (em quantidade) no mês com maior número de vendas no Olist
SELECT
  date(date_trunc('month', dtPedido)) AS dtMonth,
FROM
  silver.olist.pedido
GROUP BY
  dtMonth
ORDER BY
  count(DISTINCT idPedido) DESC
LIMIT
  1

-- COMMAND ----------

SELECT t2.idVendedor,
count(*) AS qtdeItens

FROM silver.olist.pedido AS t1

INNER JOIN silver.olist.item_pedido AS t2
ON t1.idPedido = t2.idPedido

WHERE date(date_trunc('month', t1.dtPedido)) = '2017-11-01'

GROUP BY t2.idVendedor
ORDER BY qtdeItens DESC

LIMIT 10

-- COMMAND ----------

SELECT
  t2.idVendedor,
  count(*) AS qtdeItens
FROM
  silver.olist.pedido AS t1
  INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
WHERE
  date(date_trunc('month', t1.dtPedido)) = (
    SELECT
      date(date_trunc('month', dtPedido)) AS dtMonth,
    FROM
      silver.olist.pedido
    GROUP BY
      dtMonth
    ORDER BY
      count(DISTINCT idPedido) DESC
    LIMIT
      1
  )
GROUP BY
  t2.idVendedor
ORDER BY
  qtdeItens DESC
LIMIT
  10

-- COMMAND ----------

SELECT
  *
FROM
  (
    SELECT
      date(date_trunc('month', dtPedido)) AS dtMonth,
      count(DISTINCT idPedido) AS qtdePedido
    FROM
      silver.olist.pedido
    GROUP BY
      dtMonth
    ORDER BY
      qtdePedido DESC
  )

WHERE dtMonth >= '2017-01-01'

-- COMMAND ----------

-- Total de vendas históricas (independente da categoria) dos vendedores que venderam ao menos um produto da categoria bebes da blackfriday de 2017-11-01.
SELECT
  DISTINCT t2.idVendedor
FROM
  silver.olist.pedido AS t1
  LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.produto AS t3 ON t2.idProduto = t3.idProduto
WHERE
  date(date_trunc('month', t1.dtPedido)) = '2017-11-01'
AND t3.descCategoria = 'bebes'

-- COMMAND ----------

SELECT
  idVendedor,
  count(DISTINCT idPedido) AS qtdePedido
FROM
  silver.olist.item_pedido
WHERE
  IN (
    SELECT
      DISTINCT t2.idVendedor
    FROM
      silver.olist.pedido AS t1
      LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
      LEFT JOIN silver.olist.produto AS t3 ON t2.idProduto = t3.idProduto
    WHERE
      date(date_trunc('month', t1.dtPedido)) = '2017-11-01'
      AND t3.descCategoria = 'bebes'
  )
GROUP BY
  idVendedor

-- COMMAND ----------

SELECT
  t1.idVendedor,
  count(DISTINCT t1.idPedido) AS qtdePedido
FROM
  silver.olist.item_pedido AS t1
  RIGHT JOIN (
    SELECT
      DISTINCT t2.idVendedor
    FROM
      silver.olist.pedido AS t1
      LEFT JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
      LEFT JOIN silver.olist.produto AS t3 ON t2.idProduto = t3.idProduto
    WHERE
      date(date_trunc('month', t1.dtPedido)) = '2017-11-01'
      AND t3.descCategoria = 'bebes'
  ) AS t2 ON t1.idVendedor = t2.idVendedor
GROUP BY
  t1.idVendedor

ORDER BY qtdePedido DESC
