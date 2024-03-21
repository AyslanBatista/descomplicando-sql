-- Databricks notebook source
--1.  Qual a nota (média, mínima e máxima) de cada vendedor que tiveram vendas no ano de 2017? E o percentual de pedidos avaliados com nota 5?
WITH tb_pedidos AS (
  SELECT
    DISTINCT t1.idPedido,
    t2.idVendedor
  FROM
    silver.olist.pedido AS t1
    INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  WHERE
    year(dtPedido) = 2017
),
tb_avaliacoes AS (
  SELECT
    *,
    CASE
      WHEN t2.vlNota = 5 THEN 1
      ELSE 0
    END AS flNota5
  FROM
    tb_pedidos AS t1
    INNER JOIN silver.olist.avaliacao_pedido AS t2 ON t1.idPedido = t2.idPedido
)
SELECT
  idVendedor,
  avg(t2.vlNota) avgNota,
  max(t2.vlNota) maxNota,
  min(t2.vlNota) minNota,
  avg(flNota5) AS pctNota5
FROM
  tb_avaliacoes
GROUP BY
  idVendedor

-- COMMAND ----------

--2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.
WITH tb_item_pedido AS(
  SELECT
    t2.idPedido,
    t2.idVendedor,
    t2.vlPreco
  FROM
    silver.olist.pedido AS t1
    INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  WHERE
    dtPedido >= '2017-01-01'
    AND dtPedido < '2017-07-01'
),
tb_pedido_receita AS (
  SELECT
    idVendedor,
    idPedido,
    sum(vlPreco) AS vlTotal,
    count(*)
  FROM
    tb_item_pedido
  ORDER BY
    idVendedor,
    idPedido
),
tb_final AS (
  SELECT
    idVendedor,
    avg(vlTotal) AS avgValorPedido,
    min(vlTotal) AS minValorPedido,
    max(vlTotal) AS maxValorPedido
  FROM
    tb_pedido_receita
  GROUP BY
    idVendedor
)
SELECT
  *
FROM
  tb_final

-- COMMAND ----------

--2. Calcule o valor do pedido médio, o valor do pedido mais caro e mais barato de cada vendedor que realizaram vendas entre 2017-01-01 e 2017-06-30.
WITH tb_pedido_receita AS(
  SELECT
    t2.idPedido,
    t2.idVendedor,
    sum(t2.vlPreco) AS vlTotal
  FROM
    silver.olist.pedido AS t1
    INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  WHERE
    dtPedido >= '2017-01-01'
    AND dtPedido < '2017-07-01'

  GROUP BY t2.idPedido,
    t2.idVendedor
),
tb_final AS (
  SELECT
    idVendedor,
    avg(vlTotal) AS avgValorPedido,
    min(vlTotal) AS minValorPedido,
    max(vlTotal) AS maxValorPedido
  FROM
    tb_pedido_receita
  GROUP BY
    idVendedor
)
SELECT
  *
FROM
  tb_final

-- COMMAND ----------

--3. Calcule a quantidade de pedidos por meio de pagamento que cada vendedor teve em seus pedidos entre 2017-01-01 e 2017-06-30.
WITH tb_pedido_vendedor AS (
  SELECT
    DISTINCT t2.idPedido,
    t2.idVendedor
  FROM
    silver.olist.pedido AS t1
    INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  WHERE
    dtPedido >= '2017-01-01'
    AND dtPedido < '2017-07-01'
),
tb_pedido_pagamento AS(
  SELECT
    t1.idVendedor,
    t1.idPedido,
    t2.descTipoPagamento
  FROM
    tb_pedido_vendedor AS t1
    LEFT JOIN silver.olist.pagamento_pedido AS t2 ON t1.idPedido = t2.idPedido
)
SELECT
  idVendedor,
  descTipoPagamento,
  count(DISTINCT idPedido)
FROM
  tb_pedido_pagamento
GROUP BY
  idVendedor,
  descTipoPagamento
ORDER BY
  idVendedor,
  descTipoPagamento

-- COMMAND ----------

--4. Combine a query do exercício 2 e 3 de tal forma, que cada linha seja um vendedor, e que haja colunas para cada meio de pagamento (com a quantidade de pedidos) e as colunas das estatísticas do pedido do exercício 2 (média, maior valor e menor valor).
WITH tb_pedido_receita AS (
  SELECT
    t2.idVendedor,
    t2.idPedido,
    sum(t2.vlPreco) AS vlReceita
  FROM
    silver.olist.pedido AS t1
    INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  WHERE
    dtPedido >= '2017-01-01'
    AND dtPedido < '2017-07-01'
  GROUP BY
    t2.idVendedor,
    t2.idPedido
),
tb_sumario_pedidos AS (
  SELECT
    idVendedor,
    avg(vlReceita) AS avgValorPedido,
    min(vlReceita) AS minValorPedido,
    max(vlReceita) AS maxValorPedido
  FROM
    tb_pedido_receita
  GROUP BY
    idVendedor
),
tb_pedido_pagamento AS(
  SELECT
    t1.idVendedor,
    t2.descTipoPagamento,
    count(DISTINCT t1.idPedido) AS qtdePedido
  FROM
    tb_pedido_receita AS t1
    LEFT JOIN silver.olist.pagamento_pedido AS t2 ON t1.idPedido = t2.idPedido
  GROUP BY
    t1.idVendedor,
    t2.descTipoPagamento
  ORDER BY
    t1.idVendedor,
    t2.descTipoPagamento
),
tb_pagamento_coluna AS (
  SELECT
    idVendedor,
    SUM (
      CASE
        WHEN descTipoPagamento = 'boleto' THEN qtdePedido
      END
    ) AS qtdeBoleto,
    SUM (
      CASE
        WHEN descTipoPagamento = 'credit_card' THEN qtdePedido
      END
    ) AS qtdeCredit_card,
    SUM (
      CASE
        WHEN descTipoPagamento = 'voucher' THEN qtdePedido
      END
    ) AS qtdeVoucher,
    SUM (
      CASE
        WHEN descTipoPagamento = 'debit_card' THEN qtdePedido
      END
    ) AS qtdeDebit_card
  FROM
    tb_pedido_pagamento
  GROUP BY
    idVendedor
)
SELECT
  t1.*,
  t2.qtdeBoleto,
  t2.qtdeCredit_card,
  t2.qtdeVoucher,
  t2.qtdeDebit_card
FROM
  tb_sumario_pedidos AS t1
  LEFT JOIN tb_pagamento_coluna AS t2 ON t1.idVendedor = t2.idVendedor
