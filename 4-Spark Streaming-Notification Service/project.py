import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, unix_timestamp
from pyspark.sql.types import StructType, StructField, StringType, LongType

# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
    
    # записываем df в PostgreSQL с полем feedback
    df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/de") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "subscribers_feedback") \
        .option("user", "jovyan") \
        .option("password", "jovyan") \
        .mode("append") \
        .save()
    
    # создаём df для отправки в Kafka. Сериализация в json.
    kafka_df = df.select(
        to_json(struct(
            col("restaurant_id"),
            col("adv_campaign_id"),
            col("adv_campaign_content"),
            col("adv_campaign_owner"),
            col("adv_campaign_owner_contact"),
            col("adv_campaign_datetime_start"),
            col("adv_campaign_datetime_end"),
            col("client_id"),
            col("datetime_created"),
            col("trigger_datetime_created")
        )).alias("value")
    )
    
    # отправляем сообщения в результирующий топик Kafka без поля feedback
    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", 'org.apache.kafka.common.security.scram.ScramLoginModule required username="login" password="password";') \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("topic", "student.topic.cohort7.rimdaria.out") \
        .save()
    
    # очищаем память от df
    df.unpersist()

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ",".join(
        [
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
            "org.postgresql:postgresql:42.4.0",
        ]
    )

# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
spark = SparkSession.builder \
    .appName("RestaurantSubscribeStreamingService") \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.jars.packages", spark_jars_packages) \
    .getOrCreate()

# читаем из топика Kafka сообщения с акциями от ресторанов 
restaurant_read_stream_df = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
    .option('kafka.security.protocol', 'SASL_SSL') \
    .option('kafka.sasl.jaas.config', 'org.apache.kafka.common.security.scram.ScramLoginModule required username="de-student" password="ltcneltyn";') \
    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
    .option('subscribe', 'student.topic.cohort7.rimdaria.in') \
    .load()

# определяем схему входного сообщения для json
incomming_message_schema = StructType([
    StructField("restaurant_id", StringType(), True),
    StructField("adv_campaign_id", StringType(), True),
    StructField("adv_campaign_content", StringType(), True),
    StructField("adv_campaign_owner", StringType(), True),
    StructField("adv_campaign_owner_contact", StringType(), True),
    StructField("adv_campaign_datetime_start", LongType(), True),
    StructField("adv_campaign_datetime_end", LongType(), True),
    StructField("datetime_created", LongType(), True)
])

# определяем текущее время в UTC в миллисекундах, затем округляем до секунд
current_timestamp_utc = int(round(unix_timestamp(current_timestamp())))

# десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
filtered_read_stream_df = restaurant_read_stream_df \
    .select(from_json(col("value").cast("string"), incomming_message_schema).alias("data")) \
    .select("data.*") \
    .filter((col("adv_campaign_datetime_start") <= current_timestamp_utc) & 
            (col("adv_campaign_datetime_end") >= current_timestamp_utc))

# вычитываем всех пользователей с подпиской на рестораны
subscribers_restaurant_df = spark.read \
                    .format('jdbc') \
                    .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
                    .option('driver', 'org.postgresql.Driver') \
                    .option('dbtable', 'subscribers_restaurants') \
                    .option('user', 'student') \
                    .option('password', 'de-student') \
                    .load()

# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
result_df = filtered_read_stream_df \
    .join(subscribers_restaurant_df, "restaurant_id") \
    .withColumn("trigger_datetime_created", lit(current_timestamp_utc)) \
    .withColumn("feedback", lit(""))  # добавляем пустое поле feedback для PostgreSQL

# запускаем стриминг
result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination()