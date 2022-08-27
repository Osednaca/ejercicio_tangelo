import pandas as pd
import requests 
import hashlib
import time
from basedatos import *

conn = create_connection(r"db/pythonsqlite.db")

if conn is not None:
    # create projects table
    create_table(conn, 'CREATE TABLE IF NOT EXISTS data (id INTEGER PRIMARY KEY AUTOINCREMENT, region TEXT, city_name TEXT, language TEXT, time REAL)')
else:
        print("Error! cannot create the database connection.")

response = requests.get('https://restcountries.com/v3.1/all')

df = pd.DataFrame(columns=['region','city_name','language','time'])

for country in response.json():
    start_time = time.time()
    if "languages" in country:
        language = list(country["languages"].values())[0]
    else:
        language = "N/A"
    language = hashlib.sha1(str(language).encode('utf-8')).hexdigest()
    df_new_row = pd.DataFrame({
        'region': [country["region"]],
        'city_name': [country["name"]["common"]],
        'language':[language],
        'time': [time.time() - start_time]
        })
    df = pd.concat([df, df_new_row], ignore_index=True)
    #Insert in DB
    sql = ''' INSERT INTO data(region,city_name,language,time)
              VALUES(?,?,?,?) '''
    cur = conn.cursor()
    data = (country["region"], country["name"]["common"], language,time.time() - start_time)
    cur.execute(sql, data)
    conn.commit()

#Mostrar tabla
print(df) 

#Mostrar tiempos de ejecucion de las filas del dataframe
print('Total: ' + str(df['time'].sum()))
print('Promedio: ' + str(df['time'].mean()))
print('Minimo: ' + str(df['time'].min()))
print('Maximo: ' + str(df['time'].max()))

#Guardar la data como un  archivo JSON
df.to_json('json/data.json')