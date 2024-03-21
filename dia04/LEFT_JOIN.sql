-- Databricks notebook source
SELECT
  t1.idPedido,
  t1.idPedidoItem,
  t1.idProduto,
  t1.vlPreco,
  t1.vlFrete,
  t2.descCategoria
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.produto AS t2 ON t1.idProduto = t2.idProduto

-- COMMAND ----------

SELECT
  t1.*,
  t2.descUF
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.vendedor AS t2 ON t1.idVendedor = t2.idVendedor

-- COMMAND ----------

SELECT
  t1.*,
  t2.descUF AS descUFvendedor,
  t3.descCategoria,
  t3.vlPesoGramas
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.vendedor AS t2 ON t1.idVendedor = t2.idVendedor
  LEFT JOIN silver.olist.produto AS t3 ON t1.idProduto = t3.idProduto

WHERE t2.descUF = 'SP'
