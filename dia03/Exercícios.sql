-- Databricks notebook source
--1. Qual pedido com maior valor de frete? E o menor?
SELECT
  idPedido,
  sum(vlFrete) AS totalFrete
FROM
  silver.olist.item_pedido
GROUP BY
  idPedido
ORDER BY
  totalFrete DESC

-- COMMAND ----------

--2. Qual vendedor tem mais pedidos?
SELECT
  idVendedor,
  count(DISTINCT idPedido) AS qtPedido
FROM
  silver.olist.item_pedido
GROUP BY
  idVendedor
ORDER BY
  qtPedido DESC
LIMIT
  1

-- COMMAND ----------

--3. Qual vendedor tem mais itens vendidos? E o com menos?
SELECT
  idVendedor,
  count(idProduto) AS qtItems
FROM
  silver.olist.item_pedido
GROUP BY
  idVendedor
ORDER BY qtItems DESC

-- COMMAND ----------

--4. Qual dia tivemos mais pedidos?
SELECT
  date(dtPedido) as diaPedido,
  count(DISTINCT idPedido) AS qtPedido
FROM
  silver.olist.pedido
GROUP BY
  diaPedido
ORDER BY
  qtPedido DESC

-- COMMAND ----------

--5. Quantos vendedores são do estado de São Paulo?
SELECT
  count(DISTINCT idVendedor) as qtVendedor
FROM
  silver.olist.vendedor
WHERE
  descUF = 'SP'

-- COMMAND ----------

--6. Quantos vendedores são de Presidente Prudente?
SELECT
  count(DISTINCT idVendedor) as qtVendedor
FROM
  silver.olist.vendedor
WHERE
  descCidade = 'presidente prudente'


-- COMMAND ----------

--7. Quantos clientes são do estado do Rio de Janeiro?
SELECT
  count(DISTINCT idCliente) as qtCliente
FROM
  silver.olist.cliente
WHERE
  descUF = 'RJ'

-- COMMAND ----------

--8. Quantos produtos são de construção?
SELECT
  count(DISTINCT idProduto)
FROM
  silver.olist.produto
WHERE
  descCategoria LIKE '%construcao%'

-- COMMAND ----------

--9. Qual o valor médio de um pedido? E do frete?
SELECT
  sum(vlPreco) / count(DISTINCT idPedido) as vlMedioPedido,
  sum(vlFrete) / count(DISTINCT idPedido) as vlMedioFrete
FROM
  silver.olist.item_pedido

-- COMMAND ----------

--10. Em média os pedidos são de quantas parcelas de cartão? E o valor médio por parcela?
SELECT
  avg(nrParcelas) as avgQtParcelas,
  avg(vlPagamento / nrParcelas) as avgValorParcela
FROM
  silver.olist.pagamento_pedido
WHERE
  descTipoPagamento = 'credit_card'

-- COMMAND ----------

--11. Quanto tempo em média demora para um pedido chegar depois de aprovado?
SELECT
  avg(date_diff(dtEntregue, dtAprovado,)) as avgQtDias
FROM
  silver.olist.pedido
WHERE
  descSituacao = 'delivered'

-- COMMAND ----------

--12. Qual estado tem mais vendedores?
SELECT
  descUF,
  count(DISTINCT idVendedor) AS qtVendedor
FROM
  silver.olist.vendedor
GROUP BY
  descUF
ORDER BY qtVendedor DESC


-- COMMAND ----------

--13. Qual cidade tem mais clientes?
SELECT
  descCidade,
  count(DISTINCT idCliente) AS qtCliente, -- Cliente que podem não ser únicos
  count(DISTINCT idClienteUnico) AS qtClienteDistinto -- Cliente únicos
FROM
  silver.olist.cliente
GROUP BY
  descCidade

ORDER BY qtCliente DESC

-- COMMAND ----------

--14. Qual categoria tem mais itens?

SELECT descCategoria,
count(DISTINCT idProduto) AS qtProduto

FROM silver.olist.produto

GROUP BY descCategoria

ORDER BY qtProduto DESC

-- COMMAND ----------

--15. Qual categoria tem maior peso médio de produto?
SELECT
  descCategoria,
  avg(vlPesoGramas) as pesoMedio
FROM
  silver.olist.produto
GROUP BY
  descCategoria

ORDER BY pesoMedio DESC

-- COMMAND ----------

--16. Qual a série histórica de pedidos por dia? E receita?

SELECT date(dtPedido) as diaPedido,
count(DISTINCT idPedido) as qtPedido

FROM silver.olist.pedido

GROUP BY diaPedido
ORDER BY diaPedido

-- COMMAND ----------

--17. Qual produto é o campeão de vendas? Em receita? Em quantidade?

SELECT idProduto,
count(*) as qtVenda,
sum(vlPreco) as vlReceita

FROM silver.olist.item_pedido

GROUP BY idProduto
ORDER BY qtVenda DESC
