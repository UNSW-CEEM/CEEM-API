import pandas as pd
import sqlite3
import requests
from datetime import datetime, timedelta
import time
import os

if __name__ == "__main__":
    
    pathtoDB = os.path.realpath(os.path.join(os.path.dirname(__file__), '..', 'application', 'nasa_power2.db'))

    with sqlite3.connect(pathtoDB) as con:
        LatLongList = pd.read_sql_query(con=con, sql='select Lat, Long, TS from data')
    LLL = LatLongList.groupby(['Lat', 'Long']).max()
    LLL.reset_index(inplace=True)

    for index, row in LLL.iterrows():
        start_date = row['TS']
        ThreeDaysAgo = datetime.now() - timedelta(days=3)
        end_date = ThreeDaysAgo.strftime('%Y%m%d')
        loca = {'lat': row['Lat'], 'long': row['Long']}

        url = 'https://power.larc.nasa.gov/api/temporal/daily/point?start={}&end={}&latitude={}&longitude=' \
              '{}&community=ag&parameters=CDD18_3,HDD18_3&format=json&time-standard=lst'.format(start_date, end_date, str(loca['lat']), str(loca['long']))

        print(str(index + 1) + ' out of ' + str(LLL.shape[0]) + ' locations ' + str(row['Lat']) + ' ' + str(row['Long']))
        temp_data = requests.get(url)
        temp_data = temp_data.json()

        if 'message' in temp_data:
            print(temp_data['message'])
            time.sleep(59)
        else:
            dh_temp_ws = pd.DataFrame.from_dict(temp_data['properties']['parameter'],
                                            orient='columns').reset_index()
        dh_temp_ws.rename(columns={'index': 'TS', 'CDD18_3': 'CDD', 'HDD18_3': 'HDD'}, inplace=True)
        dh_temp_ws['Lat'] = row['Lat']
        dh_temp_ws['Long'] = row['Long']
        with sqlite3.connect(pathtoDB) as con:
            dh_temp_ws.to_sql("data", con=con, if_exists='append', index=False)
