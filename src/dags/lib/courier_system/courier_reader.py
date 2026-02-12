import json
import requests
from datetime import datetime

class CourierReader:
    def __init__(self, nickname, cohort, api_key):
        self.base_url = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
        self.headers = {
            'X-Nickname': nickname,
            'X-Cohort': cohort,
            'X-API-KEY': api_key,
        }

    def get_data(self, endpoint, params):
        response = requests.get(f'{self.base_url}/{endpoint}',params=params, headers=self.headers)
        response.raise_for_status()
        return response.json()

    def load_everything(self, endpoint, params):
        limit = 50
        offset = 0
        results = []

        while True:
            params['offset'] = offset
            params['limit'] = limit

            batch = self.get_data(endpoint, params)
            if not batch:
                break
            results.extend(batch)

            offset += limit
        return results

    def save_to_stg(self, conn, table_name, data):
        prepare_data = []
        for item in data:
            obj_id = item.get('_id') or item.get('delivery_id')
            prepare_data.append((obj_id, json.dumps(item), datetime.now()))
        sql = f"""
                INSERT INTO {table_name} (object_id, object_value, update_ts)
                VALUES (%s, %s, %s)
                ON CONFLICT (object_id) DO UPDATE 
                SET object_value = EXCLUDED.object_value, update_ts = EXCLUDED.update_ts"""

        with conn.connection() as conn:
            with conn.cursor() as cur:
                cur.executemany(sql, prepare_data)