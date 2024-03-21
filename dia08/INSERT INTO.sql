-- Databricks notebook source
CREATE TABLE sandbox.linuxtips.usuarios (
  id int,
  nome string,
  idade int
);

-- COMMAND ----------

INSERT INTO
  sandbox.linuxtips.usuarios(id, nome, idade)
VALUES(1, "Téo", 31)

-- COMMAND ----------

INSERT INTO
  sandbox.linuxtips.usuarios(id, nome, idade)
VALUES(2, "Nah", 33),
  (3, "Maria", 15)

-- COMMAND ----------

INSERT INTO
  sandbox.linuxtips.usuarios(id, nome,)
VALUES(4, "Jose")

-- COMMAND ----------

INSERT INTO
  sandbox.linuxtips.usuarios
VALUES(5, "João", 21)

-- COMMAND ----------

INSERT INTO
  sandbox.linuxtips.cliente_olist
SELECT
  idCliente as id,
  descUF AS estado
FROM
  silver.olist.cliente
LIMIT
  10

-- COMMAND ----------

WITH tb_rj AS (
  SELECT
    *
  FROM
    silver.olist.cliente
  WHERE
    descUF = 'RJ'
)

INSERT INTO sandbox.linuxtips.cliente_olist

SELECT
  idCliente AS id,
  descUF AS estado
FROM
  tb_rj
LIMIT
  10
