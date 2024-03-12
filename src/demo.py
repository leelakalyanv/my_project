from pyspark.sql.functions import *
from pyspark.sql import Window

data = [("2024-01-05", "Won"),
        ("2024-01-06", "Won"),
        ("2024-01-07", "Lost"),
        ("2024-01-08", "Lost"),
        ("2024-01-09", "Won"),
        ("2024-01-10", "Lost"),
        ("2024-01-11", "Lost"),
        ("2024-01-12", "Won"),
        ("2024-01-13", "Won"),
        ("2024-01-14", "Won"),
        ("2024-01-15", "Lost"),
        ]
schema = ["Event_Date", "Event_Status"]

df1 = spark.createDataFrame(data,schema)

df1 = df1.withColumn("Event_Date", to_date(col("Event_Date")))

df2 = df1.withColumn("Event_Change", when(col("Event_Status") != lag("Event_Status").over(Window.orderBy("Event_Date")),1).otherwise(0))

df2.display()