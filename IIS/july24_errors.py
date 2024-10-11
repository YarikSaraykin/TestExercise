import pandas as pd

pd.set_option('display.max.rows', 20)
pd.set_option('display.max.columns', 15)

# Считываем все логи за июль
df_july = pd.read_csv('data//preprocessed_data//july_logs.csv')

# Преобразуем столбец к datetime
df_july['datetime'] = pd.to_datetime(df_july['datetime'], format='%Y-%m-%d %H:%M:%S')

# Оставляем только данные за 24 июля
df_24july = df_july[df_july['datetime'].dt.date == pd.to_datetime('2020-07-24').date()]

# request_types = df['cs-method'].unique() -> ['POST' 'GET' 'DELETE' 'HEAD']
# Число запросов типов 'DELETE' и 'HEAD' принебрежимо мало, время их выполнения также мало

# Рассматриваем только запросы типов POST' и 'GET'
request_types = ['POST', 'GET']

# Для каждого типа запроса формируем таблицу с разбивкой по часам, которая содержит
# количество запросов, количество неудачных запросов и количество запросов с ошибкой >= 500
dfs = {
    type: df_24july[df_24july['cs-method'] == type].groupby([pd.Grouper(key='datetime', freq='H')]).agg({
        'sc-status': ['count', lambda x: (x >= 400).sum(), lambda x: (x >= 500).sum()],
    })
        for type in request_types
}

cols = ['reqestss_count', 'failed_requests_count', 'server_errors_count']

# Убираем верхний уровень индекса
for type in request_types:
    dfs[type].columns = dfs[type].columns.droplevel(0)
    dfs[type].columns = cols
    dfs[type].reset_index(inplace=True)
    dfs[type].to_csv(f'data//preprocessed_data//july24//{type}_july24.csv', index=False)
