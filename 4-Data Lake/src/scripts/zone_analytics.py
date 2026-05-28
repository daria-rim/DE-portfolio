from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from utils import filter_recent_data, get_closest_city

def create_zone_mart():
    """
    Создает витрину данных в разрезе географических зон (городов)
    Считает статистику событий по неделям и месяцам
    """
    spark = SparkSession.builder \
        .appName("Zone Analytics Mart") \
        .getOrCreate()
    
    # Чтение данных событий и городов
    events_df = spark.read.parquet("/master/data/geo/events")
    cities_df = spark.read.option("header", "true").csv("/user/dariarim/data/geo/cities/geo.csv")
    
    # Фильтрация данных за последние 1 дней для тестирования
    events_df = filter_recent_data(events_df, days=1)
    
    # Для каждого события определяем ближайший город
    events_with_cities = get_closest_city(events_df, cities_df)
    
    # Используем даты начала периодов вместо номеров недель/месяцев
    events_with_dates = events_with_cities.withColumn("week_start", F.date_trunc("week", F.col("date"))) \
        .withColumn("month_start", F.date_trunc("month", F.col("date")))
    
    # Находим первые события пользователей для подсчета регистраций
    first_events_window = Window.partitionBy("user_id").orderBy("date")
    with_first_events = events_with_dates.withColumn("is_first_event", 
        F.row_number().over(first_events_window) == 1)
    
    # Агрегируем данные по неделям - считаем количество событий каждого типа
    weekly_stats = with_first_events.groupBy("week_start", "city") \
        .agg(
            # Количество сообщений за неделю
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("week_message"),
            # Количество реакций (лайков) за неделю
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("week_reaction"),
            # Количество подписок на каналы за неделю
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("week_subscription"),
            # Количество регистраций новых пользователей за неделю
            F.sum(F.when(F.col("is_first_event") == True, 1).otherwise(0)).alias("week_user")
        )
    
    # Агрегируем данные по месяцам
    monthly_stats = with_first_events.groupBy("month_start", "city") \
        .agg(
            # Количество сообщений за месяц
            F.sum(F.when(F.col("event_type") == "message", 1).otherwise(0)).alias("month_message"),
            # Количество реакций за месяц
            F.sum(F.when(F.col("event_type") == "reaction", 1).otherwise(0)).alias("month_reaction"),
            # Количество подписок за месяц
            F.sum(F.when(F.col("event_type") == "subscription", 1).otherwise(0)).alias("month_subscription"),
            # Количество регистраций за месяц
            F.sum(F.when(F.col("is_first_event") == True, 1).otherwise(0)).alias("month_user")
        )
    
    # Объединяем недельную и месячную статистику
    zone_mart = weekly_stats.join(monthly_stats, ["month_start", "city"], "left")
    
    # Сохраняем витрину в аналитический слой
    zone_mart.write.mode("overwrite").parquet("/user/dariarim/data/analytics/geo/zone_mart")
    
    spark.stop()

if __name__ == "__main__":
    create_zone_mart()