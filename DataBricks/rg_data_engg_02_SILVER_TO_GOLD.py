from pyspark.sql.functions import to_date, col, date_format, sum as _sum

#########################################
# Track Monthly Sales Trend & Seasonality
#########################################

fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales/")
dim_date = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimDate/")
dim_product = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimProduct")

from pyspark.sql.functions import col, sum as _sum
output_path = "/mnt/adventureworks/gold/sales_trend/"
sales_trend = (
    fact_sales.join(dim_date, fact_sales["OrderDateKey"] == dim_date["DateKey"], "inner")
    .groupBy("CalendarYear", "EnglishMonthName", "MonthNumberOfYear")
    .agg(_sum("SalesAmount").alias("TotalSales"))
    .orderBy("CalendarYear", "MonthNumberOfYear")
)
#display(sales_trend)
sales_trend.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)

#################################
# Best-Selling Products
#################################
fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales")
dim_product = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimProduct")

sales_with_product = fact_sales.join(dim_product, "ProductKey", "inner")
output_path = "/mnt/adventureworks/gold/top_products/"
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
top_products.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)

#############################
#  Sales by Geography
#############################
from pyspark.sql.functions import sum, col

fact_sales = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/FactInternetSales")
dim_customer = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimCustomer")
dim_geography = spark.read.format("delta").load("/mnt/adventureworks/silver/salesLT/DimGeography")
output_path = "/mnt/adventureworks/gold/geo_sales/"

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
geo_sales.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)
