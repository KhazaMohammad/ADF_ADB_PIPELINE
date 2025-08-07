from pyspark.sql.functions import to_date, col, date_format, sum as _sum

#########################################
# Track Monthly Sales Trend & Seasonality
#########################################

fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales/")
dim_date = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimDate/")
dim_product = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimProduct")

from pyspark.sql.functions import col, sum as _sum

sales_trend = (
    fact_sales.join(dim_date, fact_sales["OrderDateKey"] == dim_date["DateKey"], "inner")
    .groupBy("CalendarYear", "EnglishMonthName", "MonthNumberOfYear")
    .agg(_sum("SalesAmount").alias("TotalSales"))
    .orderBy("CalendarYear", "MonthNumberOfYear")
)
# display(sales_trend)

#################################
# Best-Selling Products
#################################
fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales")
dim_product = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimProduct")

sales_with_product = fact_sales.join(dim_product, "ProductKey", "inner")

top_products = (
    sales_with_product
    .groupBy("EnglishProductName")
    .agg(
        sum("SalesAmount").alias("TotalSales"),
        sum("OrderQuantity").alias("TotalQuantitySold")
    )
    .orderBy(col("TotalSales").desc())
)

#display(top_products)

#############################
#  Sales by Geography
#############################
from pyspark.sql.functions import sum, col

fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales")
dim_customer = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimCustomer")
dim_geography = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimGeography")

sales_geo = (
    fact_sales
    .join(dim_customer, "CustomerKey", "inner")
    .join(dim_geography, "GeographyKey", "inner")
)

geo_sales = (
    sales_geo
    .groupBy("EnglishCountryRegionName", "StateProvinceName", "City")
    .agg(sum("SalesAmount").alias("TotalSales"))
    .orderBy(col("TotalSales").desc())
)

#display(geo_sales)
