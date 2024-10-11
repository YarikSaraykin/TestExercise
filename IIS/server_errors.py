import pandas as pd

pd.set_option('display.max.rows', 100)
pd.set_option('display.max.columns', 5)

df_july = pd.read_csv('data//preprocessed_data//july_logs.csv')
# df_march = pd.read_csv('data//preprocessed_data//march_logs.csv')

# Преобразуем колонку datetime к типу дата и время для логов за июль
# df_july['datetime'] = pd.to_datetime(df_july['datetime'], format='%Y-%m-%d %H:%M:%S')

# И за март
# df_march['datetime'] = pd.to_datetime(df_march['datetime'], format='%Y-%m-%d %H:%M:%S')

# Анализируем ошибки 500 и 502
df_errors_july = df_july[df_july['sc-status'] > 500]

# За 28 марта ни одной 500 и 502
# df_errors_march = df_march[df_march['sc-status'] > 500]

#
df_errors_july_hour_split = df_errors_july.groupby([pd.Grouper(key='datetime', freq='H')]).agg({
    'sc-status': ['count'],
    'time-taken': ['min', 'max', 'mean'],
})

# df_errors_march_hour_split = df_errors_march.groupby([pd.Grouper(key='datetime', freq='H')]).agg({
#     'sc-status': ['count'],
#     'time-taken': ['min', 'max', 'mean'],
# })

# Оставляем только часы, в которых фигурировали ошибки сервера
df_errors_july_hour_split = df_errors_july_hour_split[df_errors_july_hour_split[('sc-status', 'count')] != 0]
# df_errors_march_hour_split = df_errors_march_hour_split[df_errors_march_hour_split[('sc-status', 'count')] != 0]

# Убираем многоуровневость у колонок
df_errors_july_hour_split.columns = ['_'.join(col).strip() for col in df_errors_july_hour_split.columns.values]

# Делаем индекс datetime колонкой
df_errors_july_hour_split.reset_index(inplace=True)

# Округляем миллисекунды до целых значений
df_errors_july_hour_split['time-taken_min'] = df_errors_july_hour_split['time-taken_min'].round()
df_errors_july_hour_split['time-taken_max'] = df_errors_july_hour_split['time-taken_max'].round()
df_errors_july_hour_split['time-taken_mean'] = df_errors_july_hour_split['time-taken_mean'].round()

df_errors_july_hour_split['time-taken_min'] = df_errors_july_hour_split['time-taken_min'].astype(int)
df_errors_july_hour_split['time-taken_max'] = df_errors_july_hour_split['time-taken_max'].astype(int)
df_errors_july_hour_split['time-taken_mean'] = df_errors_july_hour_split['time-taken_mean'].astype(int)

df_errors_july_hour_split.to_csv('data//preprocessed_data//server_errors//errors500_july_hour_split.csv', index=False)
