import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry
import time
from datetime import datetime, timedelta
import numpy as np

# Настройка кэширования и повтора запросов по коду API
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

def main():
    df = pd.read_csv('merged_rest_review.csv')

    df['date_dt'] = pd.to_datetime(df['date'])
    df['date_only'] = df['date_dt'].dt.date
    df['lat_rounded'] = df['latitude'].round(5)
    df['lon_rounded'] = df['longitude'].round(5)

    weather_cache = {}
    unique_locations = df[['lat_rounded', 'lon_rounded']].drop_duplicates()
    
    print(f"Найдено уникальных локаций: {len(unique_locations)}")

    for i, (index, row) in enumerate(unique_locations.iterrows()):
        lat = row['lat_rounded']
        lon = row['lon_rounded']

        location_dates = df[
            (df['lat_rounded'] == lat) & 
            (df['lon_rounded'] == lon)
        ]['date_only'].unique()
        
        if not len(location_dates):
            continue
            
        min_date = min(location_dates)
        max_date = max(location_dates)
        
        print(f"Обработка локации {i+1}/{len(unique_locations)}: ({lat}, {lon})")
        print(f"Запрос погоды с {min_date} по {max_date}")

        try:
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": min_date.strftime('%Y-%m-%d'),
                "end_date": max_date.strftime('%Y-%m-%d'),
                "daily": "weather_code",
                "timezone": "auto"
            }
            
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]

            daily = response.Daily()
            weather_codes = daily.Variables(0).ValuesAsNumpy()

            date_range = pd.date_range(
                start=pd.to_datetime(daily.Time(), unit="s", utc=True),
                end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=daily.Interval()),
                inclusive="left"
            ).date

            for j, date_val in enumerate(date_range):
                weather_cache[(lat, lon, date_val)] = weather_codes[j]
                
            print(f"Получено {len(weather_codes)} записей о погоде")
            
        except Exception as e:
            print(f"Ошибка для локации ({lat}, {lon}): {str(e)}")

        time.sleep(5)

    df['weather_code'] = df.apply(
        lambda row: weather_cache.get(
            (row['lat_rounded'], row['lon_rounded'], row['date_only']), 
            np.nan
        ),
        axis=1
    )

    df.to_csv('reviews_with_weather.csv', index=False)
    print("Файл успешно сохранён: reviews_with_weather.csv")