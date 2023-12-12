from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg
from pyspark.sql.window import Window
import pyspark.sql.functions as F

# Создаем Spark сессию
spark = SparkSession.builder.master('spark://127.0.0.1:7077').appName("transf_test").getOrCreate()

# Загружаем данные в DataFrame
data = [
    ("Лаптоп", 800, 5, "2023-01-01"),
    ("Смартфон", 500, 8, "2023-01-02"),
    ("Телевизор", 1200, 3, "2023-01-03"),
    ("Кофемашина", 300, 10, "2023-01-03"),
    ("Наушники", 100, 15, "2023-01-04"),
    ("Игровая консоль", 700, 6, "2023-01-05"),
    ("Холодильник", 1500, 2, "2023-01-06"),
    ("Пылесос", 200, 12, "2023-01-06"),
    ("Микроволновка", 150, 7, "2023-01-07"),
    ("Утюг", 50, 20, "2023-01-08")
]

columns = ["Товар", "Цена", "Количество", "Дата"]
df = spark.createDataFrame(data, columns)

# Выводим первые строки данных
df.show()

# Преобразовываем строку в тип данных Date
df = df.withColumn("Дата", col("Дата").cast("date"))

# Рассчитываем общую выручку
df = df.withColumn("Выручка", col("Цена") * col("Количество"))

# Анализ данных
total_revenue = df.agg(sum("Выручка").alias("ОбщаяВыручка")).collect()[0]["ОбщаяВыручка"]
best_selling_product = df.groupBy("Товар").agg(sum("Выручка").alias("ОбщаяВыручка")).orderBy(col("ОбщаяВыручка").desc()).first()
average_price = df.agg(avg("Цена").alias("СредняяЦена")).collect()[0]["СредняяЦена"]

# Выводим результаты
print(f"Общая выручка за весь период: {total_revenue}")
print(f"Товар с самой высокой выручкой: {best_selling_product['Товар']} (Выручка: {best_selling_product['ОбщаяВыручка']})")
print(f"Средняя цена товара: {average_price}")

# Визуализация данных
windowSpec = Window.orderBy("Дата")
daily_revenue = df.withColumn("ДневнаяВыручка", sum("Выручка").over(windowSpec))

# Преобразуем DataFrame в Pandas для визуализации
daily_revenue_pd = daily_revenue.select("Дата", "ДневнаяВыручка").toPandas()

# Выводим график
daily_revenue_pd.plot(x="Дата", y="ДневнаяВыручка", title="Динамика продаж по дням")

# Сохраняем результаты
df.write.mode("overwrite").parquet("output/sales_data_processed.parquet")

# Завершаем Spark сессию
spark.stop()

A