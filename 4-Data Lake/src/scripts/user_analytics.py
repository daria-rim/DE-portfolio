from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import filter_recent_data, get_closest_city

def create_user_mart():
    """
    Создает витрину данных в разрезе пользователей
    Включает актуальный город, домашний город, статистику путешествий и локальное время
    """
    # Создаем Spark сессию для обработки данных
    spark = SparkSession.builder \
        .appName("User Analytics Mart") \
        .getOrCreate()
    
    # Чтение исходных данных событий из HDFS
    events_df = spark.read.parquet("/master/data/geo/events")
    # Чтение данных о городах из CSV файла
    cities_df = spark.read.option("header", "true").csv("/user/dariarim/data/geo/cities/geo.csv")
    
    # Фильтрация данных за последние 1 дней для тестирования
    events_df = filter_recent_data(events_df, days=1)
    
    # Фильтруем только сообщения с координатами (остальные события не имеют геоданных)
    messages_df = events_df.filter(
        (F.col("event_type") == "message") &
        (F.col("lat").isNotNull()) &
        (F.col("lng").isNotNull())
    )
    
    # Для каждого сообщения находим ближайший город
    closest_city = get_closest_city(messages_df, cities_df)
    
    # Находим актуальный город - город из последнего сообщения пользователя
    last_msg_window = Window.partitionBy("user_id").orderBy(F.col("date").desc())
    act_city_df = closest_city.withColumn("rn", F.row_number().over(last_msg_window)) \
        .filter(F.col("rn") == 1) \
        .select("user_id", "city", "timezone") \
        .withColumnRenamed("city", "act_city")
    
    # Находим домашний город - город где пользователь был больше 27 дней непрерывно
    # Сначала определяем смену городов
    city_change_window = Window.partitionBy("user_id").orderBy("date")
    with_city_changes = closest_city.withColumn("prev_city", F.lag("city").over(city_change_window))
    
    # Определяем границы непрерывного пребывания в городе
    with_city_groups = with_city_changes.withColumn("city_changed", 
        F.when(F.col("city") != F.col("prev_city"), 1).otherwise(0))
    
    with_city_groups = with_city_groups.withColumn("stay_group", 
        F.sum("city_changed").over(city_change_window.rowsBetween(Window.unboundedPreceding, 0)))
    
    # Для каждой группы непрерывного пребывания находим даты начала и конца
    stay_stats = with_city_groups.groupBy("user_id", "city", "stay_group").agg(
        F.min("date").alias("stay_start"),
        F.max("date").alias("stay_end")
    ).withColumn("stay_days", F.datediff(F.col("stay_end"), F.col("stay_start")) + 1)
    
    # Находим домашний город - последний город с непрерывным пребыванием > 27 дней
    home_city_df = stay_stats.filter(F.col("stay_days") >= 27) \
        .withColumn("rn", F.row_number().over(Window.partitionBy("user_id").orderBy(F.col("stay_end").desc()))) \
        .filter(F.col("rn") == 1) \
        .select("user_id", "city") \
        .withColumnRenamed("city", "home_city")
    
    # Считаем статистику путешествий пользователя - только смены городов
    travel_cities = with_city_changes.filter(F.col("city") != F.col("prev_city")) \
        .select("user_id", "city", "date")
    
    travel_df = travel_cities.groupBy("user_id").agg(
        F.count("city").alias("travel_count"),
        F.collect_list("city").alias("travel_array")
    )
    
    # Вычисляем локальное время последнего события пользователя
    local_time_df = closest_city.withColumn(
        "local_time",
        F.from_utc_timestamp(F.col("TIME_UTC"), F.col("timezone"))
    ).groupBy("user_id").agg(F.max("local_time").alias("local_time"))
    
    # Объединяем все данные в финальную витрину
    user_mart = act_city_df.join(home_city_df, "user_id", "left") \
        .join(travel_df, "user_id", "left") \
        .join(local_time_df, "user_id", "left")
    
    # Сохраняем витрину в аналитический слой
    user_mart.write.mode("overwrite").parquet("/analytics/geo/user_mart")
    
    # Закрываем Spark сессию
    spark.stop()

if __name__ == "__main__":
    create_user_mart()