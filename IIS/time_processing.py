import pandas as pd
import numpy as np

pd.set_option('display.max.rows', 20)
pd.set_option('display.max.columns', 15)

# Читаем log-файлы за июль и март
df_july = pd.read_csv('data//preprocessed_data//july_logs.csv')
df_march = pd.read_csv('data//preprocessed_data//march_logs.csv')

# Преобразуем колонку datetime к типу дата и время для логов за июль
df_july['datetime'] = pd.to_datetime(df_july['datetime'], format='%Y-%m-%d %H:%M:%S')
# И за март
df_march['datetime'] = pd.to_datetime(df_march['datetime'], format='%Y-%m-%d %H:%M:%S')

# Колонки для сгруппированных данных
cols = [('requests', 'count'), ('requests', 'failed_requests_count'), ('time-taken', 'min'),
        ('time-taken', 'max'), ('time-taken', 'mean')]
cols = pd.MultiIndex.from_tuples(cols)

# Группируем данные: для каждого часа считаем количество запросов,
# количество неудачных запросов, минимальное, максимальное и среднее
# время выполнения запроса
# Для марта
df_march_hour_split = df_march.groupby([pd.Grouper(key='datetime', freq='H')]).agg({
    'sc-status': ['count', lambda x: (x >= 400).sum()],
    'time-taken': ['min', 'max', 'mean'],
})

# То же для июля
df_july_hour_split = df_july.groupby([pd.Grouper(key='datetime', freq='H')]).agg({
    'sc-status': ['count', lambda x: (x >= 400).sum()],
    'time-taken': ['min', 'max', 'mean'],
})

df_march_hour_split.columns = cols
df_july_hour_split.columns = cols

# Убираем многоуровневость в названиях колонок
df_march_hour_split.columns = ['_'.join(col).strip() for col in df_march_hour_split.columns.values]
df_july_hour_split.columns = ['_'.join(col).strip() for col in df_july_hour_split.columns.values]

# Переименовываем колонку для неудачных запросов для марта
df_march_hour_split.rename(
    columns={'requests_failed_requests_count': 'failed_requests_count'},
    inplace=True
)
# То же для июля
df_july_hour_split.rename(
    columns={'requests_failed_requests_count': 'failed_requests_count'},
    inplace=True
)

# Индекс - datetime. Делаем его колонкой
df_march_hour_split.reset_index(inplace=True)
df_july_hour_split.reset_index(inplace=True)

# После агрегации max, min, mean - имеют тип float
# Не считаем время с точностью до долей миллисекуннд, округляем до целых
df_march_hour_split['time-taken_min'] = df_march_hour_split['time-taken_min'].round()
df_march_hour_split['time-taken_max'] = df_march_hour_split['time-taken_max'].round()
df_march_hour_split['time-taken_mean'] = df_march_hour_split['time-taken_mean'].round()
df_march_hour_split['time-taken_mean'] = df_march_hour_split['time-taken_mean'].astype(int)
# То же для июля
df_july_hour_split['time-taken_min'] = df_july_hour_split['time-taken_min'].round()
df_july_hour_split['time-taken_max'] = df_july_hour_split['time-taken_max'].round()
df_july_hour_split['time-taken_mean'] = df_july_hour_split['time-taken_mean'].round()
df_july_hour_split['time-taken_mean'] = df_july_hour_split['time-taken_mean'].astype(int)


df_march_hour_split.to_csv('data//preprocessed_data//time_processing//march_hour_split.csv', index=False)
df_july_hour_split.to_csv('data//preprocessed_data//time_processing//july_hour_split.csv', index=False)
