import pandas as pd
import numpy as np
import glob
import chardet

pd.set_option('display.max.rows', 20)
pd.set_option('display.max.columns', 15)

# Ищем все лог-файлы
log_paths = glob.glob('data//raw_data//*.log')

# Определение кодировок
# encodings = { path: None for path in log_paths }
#
# for path in log_paths:
#     with open(path, 'rb') as f:
#         data = f.read(1000000)
#         result = chardet.detect(data)
#         encodings[path] = result['encoding']
#
# print(encodings)

# Отдельно считываем лог-файлы за март
df_march = pd.read_csv('data//raw_data//u_ex200328.log', sep=' ', header=None, encoding='iso-8859-1', skiprows=4, on_bad_lines='skip', comment='#')

# Читаем все лог-файлы, кроме мартовского
dfs_july = [pd.read_csv(path, sep=' ', header=None, encoding='iso-8859-1', skiprows=4, on_bad_lines='skip', comment='#')
       for path in log_paths if path[15:] != 'u_ex200328.log']

# Соединяем все лог-файлы в один DataFrame
dfs_july = pd.concat(dfs_july, ignore_index=True)

# Имена колонок
attributes = ['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem',
              'cs-uri-query', 's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Referer)',
              'sc-status', 'sc-substatus', 'sc-win32-status', 'time-taken']

# Заменяем имена колонок
dfs_july.columns = attributes
df_march.columns = attributes
# Обрабатываем пропущенные значения
dfs_july.replace('-', np.nan, inplace=True)
df_march.replace('-', np.nan, inplace=True)

# Преобразуем date и time в datetime
dfs_july['datetime'] = pd.to_datetime(dfs_july['date'] + ' ' + dfs_july['time'], format='%Y-%m-%d %H:%M:%S')
dfs_july.insert(0, 'datetime', dfs_july.pop('datetime'))
df_march['datetime'] = pd.to_datetime(df_march['date'] + ' ' + df_march['time'], format='%Y-%m-%d %H:%M:%S')
df_march.insert(0, 'datetime', df_march.pop('datetime'))

# Преобразуем числовые данные в int
dfs_july['s-port'] = dfs_july['s-port'].astype(int)
dfs_july['sc-status'] = dfs_july['sc-status'].astype(int)
dfs_july['sc-substatus'] = dfs_july['sc-substatus'].astype(int)
dfs_july['sc-win32-status'] = dfs_july['sc-win32-status'].astype(int)
dfs_july['time-taken'] = dfs_july['time-taken'].astype(int)

df_march['s-port'] = df_march['s-port'].astype(int)
df_march['sc-status'] = df_march['sc-status'].astype(int)
df_march['sc-substatus'] = df_march['sc-substatus'].astype(int)
df_march['sc-win32-status'] = df_march['sc-win32-status'].astype(int)
df_march['time-taken'] = df_march['time-taken'].astype(int)

# print(df['s-ip'].unique()) -> ip сервера(уникальное значение)
dfs_july.drop(columns=['s-ip', 'date', 'time'], inplace=True)
df_march.drop(columns=['s-ip', 'date', 'time'], inplace=True)

# Не очевидно, что считать дубликатом
# init_len = len(df)
# df.drop_duplicates(inplace=True)
# new_len = len(df)
# print(f'{init_len - new_len} дубликатов удалено')

# Отдельно сохраняем мартовские логи
df_march.to_csv('data//preprocessed_data//march_logs.csv', index=False)

# Сохраняем июльские логи
dfs_july.to_csv('data//preprocessed_data//july_logs.csv', index=False)
