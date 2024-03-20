-- Databricks notebook source
-- DBTITLE 1,Contagem de linhas da tabela
SELECT
  count(*),
  count(1)
FROM
  silver.olist.pedido

-- COMMAND ----------

SELECT
  count(descSituacao), -- linhas não nulas deste campo
  count(DISTINCT descSituacao) -- linhas distintas deste campo
FROM
  silver.olist.pedido

-- COMMAND ----------

SELECT
  count(idPedido),
  count(DISTINCT idPedido),
  count(*),
  count(1)
FROM
  silver.olist.pedido
