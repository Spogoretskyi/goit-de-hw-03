from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, round


spark = SparkSession.builder.appName("GOIT DE HW03").getOrCreate()

# 1. Завантажте та прочитайте кожен CSV-файл як окремий DataFrame.
users = spark.read.option("header", "true").csv("data\\users.csv", inferSchema=True)
users.show()

purchases = spark.read.option("header", "true").csv(
    "data\\purchases.csv", inferSchema=True
)
purchases.show()

products = spark.read.option("header", "true").csv(
    "data\\products.csv", inferSchema=True
)
products.show()

# 2. Очистіть дані, видаляючи будь-які рядки з пропущеними значеннями.
users = users.dropna()
users.show()

purchases = purchases.dropna()
purchases.show()

products = products.dropna()
products.show()

merged_dfs = (
    purchases.join(products, on="product_id", how="inner")
    .join(users, on="user_id", how="inner")
    .withColumn("total_amount", col("quantity") * col("price"))
)
merged_dfs.show()

# 3. Визначте загальну суму покупок за кожною категорією продуктів.
total_sales_by_category = merged_dfs.groupBy("category").agg(
    spark_sum("total_amount").alias("total_sales")
)
total_sales_by_category.show()

# 4. Визначте суму покупок за кожною категорією продуктів для вікової категорії від 18 до 25 включно.
total_sales_by_category_age_18_25 = (
    merged_dfs.filter((col("age") >= 18) & (col("age") <= 25))
    .groupBy("category")
    .agg(spark_sum("total_amount").alias("total_sales_18_25"))
)
total_sales_by_category_age_18_25.show()

# 5. Визначте частку покупок за кожною категорією товарів від сумарних витрат для вікової категорії від 18 до 25 років
total_sales_18_25 = total_sales_by_category_age_18_25.agg(
    spark_sum("total_sales_18_25").alias("grand_total_18_25")
).collect()[0][0]

percentage_sales = total_sales_by_category_age_18_25.withColumn(
    "percentage", round((col("total_sales_18_25") / total_sales_18_25) * 100, 2)
)
percentage_sales.show()

# 6. Виберіть 3 категорії продуктів з найвищим відсотком витрат споживачами віком від 18 до 25 років.
top_3_categories = percentage_sales.orderBy(col("percentage").desc()).limit(3)
top_3_categories.show()
