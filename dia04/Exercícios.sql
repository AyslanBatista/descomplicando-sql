-- Databricks notebook source
--1. Qual categoria tem mais produtos vendidos?
SELECT
  t2.descCategoria,
  count(*) as qtdeCategoria --count(DISTINCT t1.idPedido) as qtdePEdidos
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.produto AS t2 ON t1.idPedido = t2.idProduto
GROUP BY
  t2.descCategoria
ORDER BY
  qtdeCategoria DESC
LIMIT
  1

-- COMMAND ----------

--2. Qual categoria tem produtos mais caros, em média? E Mediana?
SELECT
  t2.descCategoria,
  avg(t1.vlPreco) AS avgPreco,
  percentile(t1.vlPreco, 0.5) AS medianPreco
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.produto AS t2 ON t1.idProduto = t2.idProduto
GROUP BY
  t2.descCategoria
ORDER BY
  avgPreco DESC
LIMIT
  1

-- COMMAND ----------

--3. Qual categoria tem maiores fretes, em média?

SELECT t2.descCategoria,
avg(t1.vlFrete) as avgFrete

FROM silver.olist.item_pedido AS t1

LEFT JOIN silver.olist.produto AS t2
ON t1.idProduto = t2.idProduto

GROUP BY t2.descCategoria
ORDER BY avgFrete DESC

LIMIT 1


-- COMMAND ----------

--4. Os clientes de qual estado pagam mais frete, em média?
SELECT
  t3.descUF,
  sum(t1.vlFrete) / count(DISTINCT t1.idPedido) as avgFrete,
  avg(t1.vlFrete) as avgFreteItem,
  sum(t1.vlFrete) / count(DISTINCT t2.idCliente) as avgFreteCliente
FROM
  silver.olist.item_pedido AS t1
  INNER JOIN silver.olist.pedido AS t2 ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.cliente AS t3 ON t2.idCliente = t3.idCliente
GROUP BY
  t3.descUF
ORDER BY
  avgFrete DESC
LIMIT
  1

-- COMMAND ----------

--5. Clientes de quais estados avaliam melhor, em média? Proporção de 5?
SELECT
  t3.descUF,
  avg(t1.vlNota) as avgNota,
  avg(CASE WHEN t1.vlNota = 5 THEN 1 ELSE 0 END) as prop5
FROM
  silver.olist.avaliacao_pedido as t1
  INNER JOIN silver.olist.pedido as t2 ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.cliente as t3 ON t2.idCliente = t3.idCliente

GROUP BY t3.descUF
ORDER BY prop5 DESC
LIMIT 1

-- COMMAND ----------

--6. Vendedores de quais estados têm as piores reputações?
SELECT
  t1.idPedido,
  t2.idVendedor,
  t3.descUF,
  avg(t1.vlNota) AS avgNota
FROM
  silver.olist.avaliacao_pedido AS t1
  INNER JOIN silver.olist.item_pedido AS t2 ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.vendedor AS t3 ON t2.idVendedor = t3.idVendedor

  GROUP BY t3.descUF
  ORDER BY avgNota

  LIMIT 1

-- COMMAND ----------

--7. Quais estados de clientes levam mais tempo para a mercadoria chegar?
SELECT
  t2.descUF,
  avg(
    t1.dtEntregue date_diff(t1.dtEntregue, t1.dtPedido)
  ) AS qtdeDias
FROM
  silver.olist.pedido AS t1
  LEFT JOIN silver.olist.cliente AS t2 ON t1.idCliente = t2.idCliente
WHERE
  t1.dtEntregue IS NOT NULL
GROUP BY
  t2.descUF
ORDER BY
  qtdeDias

-- COMMAND ----------

--8. Qual meio de pagamento é mais utilizado por clientes do RJ?
SELECT
  t1.descTipoPagamento,
  count(DISTINCT t1.idPedido) AS qtdPedidos
FROM
  silver.olist.pagamento_pedido AS t1
  INNER JOIN silver.olist.pedido AS t2 ON t1.idPedido = t2.idPedido
  LEFT JOIN silver.olist.cliente as t3 ON t2.idCliente = t3.idCliente
WHERE
  t3.descUF = 'RJ'
GROUP BY
  t1.descTipoPagamento
ORDER BY
  qtdPedidos
LIMIT
  1

-- COMMAND ----------

--9. Qual estado sai mais ferramentas?
SELECT
  t3.descUF,
  count(*) AS qtdeProdutoVendido
FROM
  silver.olist.item_pedido AS t1
  LEFT JOIN silver.olist.produto AS t2 ON t1.idProduto = t2.idProduto
  LEFT JOIN silver.olist.vendedor AS t3 ON t1.idVendedor = t3.idVendedor
WHERE
  t2.descCategoria LIKE '%ferramentas%'
GROUP BY
  t3.descUF
ORDER BY
  qtdeProdutoVendido DESC
LIMIT
  1

-- COMMAND ----------

--10. Qual estado tem mais compras por cliente?
SELECT
  t2.descUF,
  count(DISTINCT t1.idPedido) AS qtdePedido,
  count(DISTINCT t2.idClienteUnico) AS qtdeClienteUnico,
  count(DISTINCT t1.idPedido) / count(DISTINCT t2.idClienteUnico) AS avgPedidoCliente
FROM
  silver.olist.pedido AS t1
  LEFT JOIN silver.olist.cliente AS t2 ON t1.idCliente = t2.idCliente
GROUP BY
  t2.descUF
ORDER BY
  avgPedidoCliente DESC
