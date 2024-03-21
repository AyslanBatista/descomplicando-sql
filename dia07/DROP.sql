-- Databricks notebook source
DROP TABLE sandbox.linuxtips.top50_pedido_ayslan

-- COMMAND ----------

DROP TABLE IF EXISTS sandbox.linuxtips.top50_pedido_ayslan;

CREATE TABLE IF NOT EXISTS sandbox.linuxtips.top50_pedido_ayslan AS
SELECT
  *
FROM
  silver.olist.pedido
LIMIT
  5;
