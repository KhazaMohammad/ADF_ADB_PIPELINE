from pyspark.sql.functions import date_format, from_utc_timestamp,col
from pyspark.sql.types import TimestampType

# Identifying the date columns and typecast them

for i in dbutils.fs.ls("/mnt/adventureworks/bronze/"):
    print("Bronze File Path is: ", i.path)
    df_table=spark.read.parquet(i.path)
    df_columns=df_table.columns
    
    for column in df_columns:
       if "date" in column.lower() and "key" not in column.lower():
           print("column name to be date formatted is: ", column)
           df_table = df_table.withColumn(column, date_format(
                                        from_utc_timestamp(col(column).cast(TimestampType()), "UTC"),"yyyy-MM-dd")
                                          )
    output_path = "/mnt/adventureworks/silver/salesLT/"+i.name
    print("silver File Path is: ", output_path)
    df_table.write.format("delta").option("overwriteSchema", "true").mode("overwrite").save(output_path)
