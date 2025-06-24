import requests
import csv
from datetime import datetime
from collections import Counter
import pandas as pd

def get_public_holidays(year, country_code):
    url = f"https://date.nager.at/api/v3/publicholidays/{year}/{country_code}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Ошибка при получении данных за {year} год: {str(e)}")
        return None

def process_holiday_data(holiday):
    return {
        'date': holiday.get('date', ''),
        'localName': holiday.get('localName', ''),
        'name': holiday.get('name', ''),
        'countryCode': holiday.get('countryCode', ''),
        'fixed': holiday.get('fixed', False),
        'global': holiday.get('global', False),
        'counties': ';'.join(holiday.get('counties', [])) or '',
        'launchYear': holiday.get('launchYear'),
        'types': ';'.join(holiday.get('types', [])) or ''
    }

def save_holidays_to_csv(holidays, filename):
    fieldnames = [
        'date', 'localName', 'name', 'countryCode', 
        'fixed', 'global', 'counties', 'launchYear', 'types'
    ]
    
    df = pd.DataFrame(holidays)

    df['fixed'] = df['fixed'].astype(bool)
    df['global'] = df['global'].astype(bool)
    
    df.to_csv(filename, index=False, encoding='utf-8')
    print(f"Данные успешно сохранены в {filename}")

def main():
    country_code = "US"
    start_year = 2005
    end_year = 2025
    output_file = "us_holidays_2005-2025.csv"
    
    all_holidays = []
    
    for year in range(start_year, end_year + 1):
        print(f"Обработка {year} года...", end=' ', flush=True)
        holidays = get_public_holidays(year, country_code)
        processed = [process_holiday_data(h) for h in holidays]
        all_holidays.extend(processed)
    
    save_holidays_to_csv(all_holidays, output_file)