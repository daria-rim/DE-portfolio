from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from utils import filter_recent_data, get_closest_city, calculate_distance

def create_friend_recommendations():
    """
    Создает витрину для системы рекомендации друзей
    Находит пары пользователей которые подходят для рекомендации
    """
    spark = SparkSession.builder \
        .appName("Friend Recommendations") \
        .getOrCreate()
    
    # Чтение данных событий и городов
    events_df = spark.read.parquet("/master/data/geo/events")
    cities_df = spark.read.option("header", "true").csv("/user/dariarim/data/geo/cities/geo.csv")
    
    # Фильтрация данных за последние 1 дней
    events_df = filter_recent_data(events_df, days=1)
    
    # Получаем координаты пользователей из их сообщений
    messages_df = events_df.filter(
        (F.col("event_type") == "message") &
        (F.col("lat").isNotNull()) &
        (F.col("lng").isNotNull())
    )
    
    # Для каждого пользователя определяем его последние координаты и город
    last_coord_window = Window.partitionBy("user_id").orderBy(F.col("date").desc())
    users_last_coords = get_closest_city(messages_df, cities_df) \
        .withColumn("rn", F.row_number().over(last_coord_window)) \
        .filter(F.col("rn") == 1) \
        .select("user_id", "lat", "lon", "city", "timezone")
    
    # Находим подписки пользователей на каналы
    subscriptions = events_df.filter(F.col("event_type") == "subscription") \
        .select("user_id", "channel_id")
    
    # Ищем пользователей которые подписаны на одни и те же каналы
    common_channels = subscriptions.alias("s1") \
        .join(subscriptions.alias("s2"), "channel_id") \
        .filter(F.col("s1.user_id") != F.col("s2.user_id")) \
        .select(
            F.col("s1.user_id").alias("user_left"),
            F.col("s2.user_id").alias("user_right"),
            "channel_id"
        )
    
    # Находим все сообщения между пользователями
    messages = events_df.filter(F.col("event_type") == "message") \
        .select("user_id", "receiver_id")
    
    # Исключаем пары которые уже переписывались друг с другом
    never_messaged = common_channels.join(
        messages,
        ((F.col("user_left") == F.col("user_id")) & (F.col("user_right") == F.col("receiver_id"))) |
        ((F.col("user_left") == F.col("receiver_id")) & (F.col("user_right") == F.col("user_id"))),
        "left_anti"
    )
    
    # Добавляем последние координаты обоих пользователей каждой пары
    with_coords = never_messaged \
        .join(users_last_coords.alias("ul"), F.col("user_left") == F.col("ul.user_id")) \
        .join(users_last_coords.alias("ur"), F.col("user_right") == F.col("ur.user_id"))
    
    # Вычисляем расстояние между пользователями каждой пары
    with_distance = with_coords.withColumn(
        "distance_km",
        calculate_distance(F.col("ul.lat"), F.col("ul.lng"), F.col("ur.lat"), F.col("ur.lng"))
    ).filter(F.col("distance_km") <= 1)
    
    # Формируем финальные рекомендации
    recommendations = with_distance.withColumn("processed_dttm", F.current_timestamp()) \
        .withColumn("local_time", F.from_utc_timestamp(F.current_timestamp(), F.col("ul.timezone"))) \
        .select(
            "user_left",
            "user_right",
            "processed_dttm",
            F.col("ul.city").alias("zone_id"),
            "local_time"
        )
    
    # Удаляем дубликаты пар
    recommendations = recommendations.withColumn(
        "user_pair",
        # Создаем уникальный идентификатор пары (сортированный по ID)
        F.when(F.col("user_left") < F.col("user_right"), 
               F.concat(F.col("user_left"), F.lit("_"), F.col("user_right")))
         .otherwise(F.concat(F.col("user_right"), F.lit("_"), F.col("user_left")))
    ).dropDuplicates(["user_pair"]).drop("user_pair")
    
    # Сохраняем рекомендации в аналитический слой
    recommendations.write.mode("overwrite").parquet("/analytics/geo/friend_recommendations")
    
    spark.stop()

if __name__ == "__main__":
    create_friend_recommendations()