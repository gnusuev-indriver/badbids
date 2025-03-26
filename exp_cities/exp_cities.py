import pandas as pd
from get_data import get_data

# Получаем все данные одним запросом
df = get_data(start_date='2025-02-01', stop_date='2025-02-28')

# Сохраняем результаты
df.to_csv('badbids/exp_cities/badbids_exp_cities_all.csv', index=False)