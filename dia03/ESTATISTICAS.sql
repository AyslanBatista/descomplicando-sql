-- Databricks notebook source
SELECT
  sum(vlPreco) / count(vlPreco), -- média na mão
  avg(vlPreco), -- média aritimética
  min(vlPreco), -- mínimo de um campo

  max(vlFrete), -- máximo de frete pago
  std(vlFrete), -- desvio padrão
  percentile(vlFrete, 0.5), -- mediana
  avg(vlFrete), -- média

FROM
  silver.olist.item_pedido
