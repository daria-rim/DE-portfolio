from datetime import datetime, timedelta
from pyspark.sql import functions as F
from pyspark.sql.window import Window

def filter_recent_data(df, days=1, reference_date=None):
    """
    Фильтрует данные за последние N дней для экономии ресурсов
    """
    if reference_date is None:
        reference_date = datetime.now()
    elif isinstance(reference_date, str):
        reference_date = datetime.strptime(reference_date, '%Y-%m-%d')
    
    start_date = (reference_date - timedelta(days=days)).strftime('%Y-%m-%d')
    return df.filter(F.col("date") >= start_date)

def calculate_distance(lat1, lng1, lat2, lng2):
    """
    Вычисляет расстояние между двумя точками на сфере по формуле Хаверсина
    """
    R = 6371  # Радиус Земли в километрах
    
    # Переводим градусы в радианы для тригонометрических функций
    lat1_rad = F.radians(lat1)
    lng1_rad = F.radians(lng1)
    lat2_rad = F.radians(lat2)
    lng2_rad = F.radians(lng2)
    
    # Вычисляем разницы координат в радианах
    dlat = lat2_rad - lat1_rad
    dlng = lng2_rad - lng1_rad
    
    # Формула Хаверсина для расчета расстояния на сфере
    a = F.sin(dlat/2)**2 + F.cos(lat1_rad) * F.cos(lat2_rad) * F.sin(dlng/2)**2
    c = 2 * F.asin(F.sqrt(a))
    
    # Возвращаем расстояние в километрах
    return R * c

def get_closest_city(events_df, cities_df):
    """
    Находит ближайший город для каждого события на основе координат
    """
    # Переименовываем колонки городов чтобы избежать конфликтов имен
    cities_renamed = cities_df.withColumnRenamed("lat", "city_lat") \
                             .withColumnRenamed("lng", "city_lng")
    
    # Декартово произведение - каждое событие соединяется с каждым городом
    cross_join = events_df.crossJoin(cities_renamed)
    
    # Вычисляем расстояние от события до каждого города
    with_distance = cross_join.withColumn(
        "distance",
        calculate_distance(F.col("lat"), F.col("lng"), F.col("city_lat"), F.col("city_lng"))
    )
    
    # Для каждого события находим город с минимальным расстоянием
    window = Window.partitionBy("message_id").orderBy("distance")
    closest_city = with_distance.withColumn("rn", F.row_number().over(window)) \
        .filter(F.col("rn") == 1) \
        .drop("rn", "distance", "city_lat", "city_lng")
    
    return closest_city