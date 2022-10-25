-- Big project for SQL
-- Link instruction: https://docs.google.com/spreadsheets/d/1WnBJsZXj_4FDi2DyfLH1jkWtfTridO2icWbWCh7PLs8/edit#gid=0


-- Query 01: calculate total visit, pageview, transaction and revenue for Jan, Feb and March 2017 order by month
#standardSQL
--select
--    count(distinct fullVisitorId)
--FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
--Where _table_suffix between '20170101' and '20170331'

SELECT
  format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS MONTH
  ,SUM(totals.visits) AS visits
  ,SUM(totals.pageviews) AS pageviews
  ,SUM(TOTALS.TRANSACTIONS) AS TRANSACTIONS
  ,SUM(totals.totalTransactionRevenue)/1000000 AS revenue
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170101'and'20170331'
GROUP BY MONTH
ORDER BY MONTH



-- Query 02: Bounce rate per traffic source in July 2017
#standardSQL
SELECT
  TRAFFICSOURCE.SOURCE AS SOURCE
  ,COUNT(fullVisitorId) AS VISITS
  ,SUM(totals.bounces) AS TOTAL_NO_OF_BOUNCES
  ,ROUND(SUM(totals.bounces)/COUNT(fullVisitorId)*100.00,8) AS BOUNCE_RATE
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
GROUP BY SOURCE
ORDER BY
  VISITS DESC

-- Query 3: Revenue by traffic source by week, by month in June 2017
SELECT  
  CASE WHEN format_date('%Y%m',parse_date('%Y%m%d',DATE)) = format_date('%Y%m',parse_date('%Y%m%d',DATE)) THEN 'Month' END AS TIME_TYPE
  ,format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS TIME
  ,trafficSource.source AS SOURCE
  ,SUM(totals.totalTransactionRevenue)/1000000 AS REVENUE
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
GROUP BY SOURCE,TIME,TIME_TYPE

UNION ALL

SELECT  
  CASE WHEN format_date('%Y%U',parse_date('%Y%m%d',DATE)) = format_date('%Y%U',parse_date('%Y%m%d',DATE)) THEN 'Week' END AS TIME_TYPE
  ,format_date('%Y%W',parse_date('%Y%m%d',DATE)) AS TIME
  ,trafficSource.source AS SOURCE
  ,SUM(totals.totalTransactionRevenue)/1000000 AS REVENUE
FROM
  `bigquery-public-data.google_analytics_sample.ga_sessions_201706*` 
GROUP BY SOURCE,TIME,TIME_TYPE

ORDER BY REVENUE DESC

--Query 04: Average number of product pageviews by purchaser type (purchasers vs non-purchasers) in June, July 2017. Note: totals.transactions >=1 for purchaser and totals.transactions is null for non-purchaser
#standardSQL
WITH A AS 
(SELECT 
  fullVisitorId AS PURCHASER
  ,SUM(totals.pageviews) AS TOTAL_PV_PER_PURCHASERS
  ,format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS MONTH
FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170601'and'20170731' AND totals.transactionS >=1
GROUP BY fullVisitorId,MONTH)

, B AS
(SELECT 
  fullVisitorId AS NON_PURCHASERS
  ,SUM(totals.pageviews) AS TOTAL_PV_PER_NON_PURCHASERS
  ,format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS MONTH
FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_*`
WHERE
  _TABLE_SUFFIX BETWEEN '20170601'and'20170731' AND totals.transactionS IS NULL
GROUP BY fullVisitorId,MONTH)

SELECT 
  A.MONTH AS MONTH
  ,ROUND(SUM(A.TOTAL_PV_PER_PURCHASERS) / COUNT (PURCHASER),8) AS NO_PAGEVIEW_PURCHASE
  ,ROUND(SUM(B.TOTAL_PV_PER_NON_PURCHASERS)/ COUNT (NON_PURCHASERS),8) AS NO_PAGEVIEW_NON_PURCHASE
FROM A
FULL JOIN B 
ON B.MONTH =A.MONTH
GROUP BY A.MONTH
ORDER BY A.MONTH




-- Query 05: Average number of transactions per user that made a purchase in July 2017
#standardSQL
WITH C AS 
(SELECT 
  format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS MONTH
  ,fullVisitorId AS PURCHASER
  ,SUM(totals.transactions) AS TRANSACTION_PER_USERS
FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
WHERE totals.transactions >=1
GROUP BY MONTH , PURCHASER)

SELECT 
  C.MONTH AS MONTH
  ,AVG (C.TRANSACTION_PER_USERS)AS AVG_TRANSACTION_PER_USERS
FROM C
GROUP BY MONTH

-- Query 06: Average amount of money spent per session
#standardSQL
-- KHÁC KẾT QỦA SO VỚI ĐÁP ÁN
WITH E AS 
(SELECT 
  format_date('%Y%m',parse_date('%Y%m%d',DATE)) AS MONTH
  ,fullVisitorId AS USER
  ,SUM(totals.visits) AS TOTAL_VISITS_EACH_USER
  ,SUM(totals.transactionRevenue) AS TOTAL_REVENUE_EACH_USER
FROM
 `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`
 WHERE totals.transactions IS NOT NULL
 GROUP BY MONTH, USER )

SELECT 
  MONTH
  ,SUM(TOTAL_REVENUE_EACH_USER)/ SUM(TOTAL_VISITS_EACH_USER) AS AVG_TOTAL_REVENUE 
FROM E
 GROUP BY MONTH


-- Query 07: Other products purchased by customers who purchased product "YouTube Men's Vintage Henley" in July 2017. Output should show product name and the quantity was ordered.
#standardSQL

SELECT 
  v2ProductName AS other_purchased_products
  ,SUM(productQuantity) AS QUANTITY
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`,
UNNEST (HITS) AS HITS,
UNNEST (HITS.PRODUCT) AS PRODUCT
WHERE fullVisitorId IN (
  SELECT
    fullVisitorId
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_201707*`, 
  UNNEST (HITS) AS HITS,
  UNNEST (HITS.PRODUCT) AS PRODUCT
  WHERE PRODUCT.V2PRODUCTNAME= "YouTube Men's Vintage Henley"
  AND productRevenue IS NOT NULL
)
AND productRevenue IS NOT NULL
AND v2ProductName !=  "YouTube Men's Vintage Henley"
GROUP BY other_purchased_products
ORDER BY QUANTITY DESC

--Query 08: Calculate cohort map from pageview to addtocart to purchase in last 3 month. For example, 100% pageview then 40% add_to_cart and 10% purchase.
#standardSQL
WITH VIEW_TABLE AS(
SELECT  
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',DATE)) AS MONTH
  ,ITEM.productSku AS PRODUCT_SKU
  ,COUNT(eCommerceAction.action_type) AS num_product_view
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST (HITS) AS HITS,
UNNEST (HITS.PRODUCT) AS PRODUCT
WHERE 
  _TABLE_SUFFIX BETWEEN '20170101'and'20170331'
AND eCommerceAction.action_type ='2'
GROUP BY MONTH,  PRODUCT_SKU )


,A2C_TABLE AS(
SELECT  
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',DATE)) AS MONTH
  ,ITEM.productSku AS PRODUCT_SKU
  ,COUNT(eCommerceAction.action_type) AS num_addtocart
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST (HITS) AS HITS,
UNNEST (HITS.PRODUCT) AS PRODUCT
WHERE 
  _TABLE_SUFFIX BETWEEN '20170101'and'20170331'
AND eCommerceAction.action_type ='3'
GROUP BY PRODUCT_SKU, MONTH)

,PURCHASE_TABLE AS(
SELECT  
  FORMAT_DATE('%Y%m',PARSE_DATE('%Y%m%d',DATE)) AS MONTH
  ,ITEM.productSku AS PRODUCT_SKU
  ,COUNT(eCommerceAction.action_type) AS num_purchase
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
UNNEST (HITS) AS HITS,
UNNEST (HITS.PRODUCT) AS PRODUCT
WHERE 
  _TABLE_SUFFIX BETWEEN '20170101'and'20170331'
AND eCommerceAction.action_type ='6'
GROUP BY PRODUCT_SKU, MONTH)

SELECT 
  VT.MONTH AS MONTH
  ,VT.num_product_view AS num_product_view
  ,ATB.num_addtocart AS num_addtocart
  ,PT.num_purchase  AS num_purchase
  ,ROUND(num_addtocart/num_product_view *100.00,2) AS add_to_cart_rate
  ,ROUND(num_purchase/num_product_view *100.00,2) AS add_to_cart_rate
FROM VIEW_TABLE AS VT
INNER JOIN A2C_TABLE AS ATB
ON VT. MONTH = ATB.MONTH 
INNER JOIN PURCHASE_TABLE AS PT
ON ATB.MONTH = PT. MONTH 
ORDER BY MONTH