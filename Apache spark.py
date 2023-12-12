from pyspark.sql import SparkSession

spark = SparkSession.builder.master('spark://127.0.0.1:7077').appName("transf_test").getOrCreate()

persons = spark.createDataFrame(
    [

        [0, 'Nadir', '680r'],
        [1, 'Ayaz', '680r'],
        [2, 'Seymur', '680r']

    ]
).toDF('Id', "Name", "Group")

persons.show()
persons.printSchema()

