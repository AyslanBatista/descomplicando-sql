-- Databricks notebook source
WITH tb_receita_diaria AS (
  SELECT
    date(t2.dtPedido) AS dataPedido,
    sum(t1.vlPreco) AS receitaDia
  FROM
    silver.olist.item_pedido AS t1
    INNER JOIN silver.olist.pedido AS t2 ON t1.idPedido = t2.idPedido
  GROUP BY
    dataPedido
  ORDER BY
    dataPedido
)
SELECT
  *, sum(receitaDia) OVER (PARTITION BY 1 ORDER BY dataPedido) AS receitaAcum
FROM
  tb_receita_diaria

-- COMMAND ----------

WITH tb_vendedor_dia AS (
  SELECT
    date(t2.dtPedido) AS dataPedido,
    t1.idVendedor,
    sum(t1.vlPreco) AS vlReceita
  FROM
    silver.olist.item_pedido AS t1
    INNER JOIN silver.olist.pedido AS t2 ON t1.idPedido = t2.idPedido
  GROUP BY
    dataPedido,
    t1.idVendedor
  ORDER BY
    t1.idVendedor,
    dataPedido
)
SELECT
  *,
  sum(vlReceita) OVER (
    PARTITION BY idVendedor
    ORDER BY
      dataPedido
  ) AS receitaAcum
FROM
  tb_vendedor_dia
