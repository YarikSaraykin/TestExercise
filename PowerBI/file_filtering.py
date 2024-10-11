import pandas as pd
import numpy as np

pd.set_option('display.max.rows', 100)
pd.set_option('display.max.columns', 30)

# Читаем исходный файл
path = 'data//Показатели.xlsx'
df = pd.read_excel(path, usecols='C, G:P', skiprows=3)

# Очищаем первую колонку от названий разделов
df['Unnamed: 2'].replace(to_replace=r'Раздел*', value=np.nan, regex=True, inplace=True)

# Помещаем названия разделов под year_fact_forecast
sections = ['Раздел1', 'Раздел2', 'Раздел3']
sections_mask = df['Unnamed: 2'].str.contains('year_fact_forecast', na=False)
df.loc[sections_mask.shift(1).fillna(False), 'Unnamed: 2'] = sections


df.columns = df.iloc[0]
df = df[1:]
df = df[(df != df.columns).any(axis=1)]
df.columns.name = None
df.reset_index(drop=True, inplace=True)

df.dropna(inplace=True)

df = df[~((df['year_fact_forecast'].str.startswith('Показатель')) & df.iloc[:, 1:].isna().all(axis=1))]

def determine_type(value):
    if value.startswith('Раздел'):
        return 'Раздел'
    elif value.startswith('Подраздел'):
        return 'Подраздел'
    else:
        return 'Показатель'

# Применяем функцию к колонке
df['type'] = df['year_fact_forecast'].apply(determine_type)

# Создаем колонки Раздел, Подраздел, Показатель
df['Раздел'] = np.where(df['type'] == 'Раздел', df['year_fact_forecast'], np.nan)
df['Подраздел'] = np.where(df['type'] == 'Подраздел', df['year_fact_forecast'], np.nan)
df['Показатель'] = np.where(df['type'] == 'Показатель', df['year_fact_forecast'], np.nan)

# Заполняем пропуски методом forward fill
df['Раздел'] = df['Раздел'].ffill()
df['Подраздел'] = df['Подраздел'].ffill()

df = df[~df['type'].isin(['Раздел', 'Подраздел'])]
df.drop(columns=['type', 'year_fact_forecast'], inplace=True)

columns_order = ['Раздел','Подраздел', 'Показатель'] + [col for col in df.columns if col not in ['Раздел','Подраздел', 'Показатель']]
df = df.reindex(columns=columns_order)

df.rename(
    columns={'Показатель': 'year_fact_forecast'},
    inplace=True
)

df.to_excel('data//Показатели_обработанный.xlsx', index=False)
