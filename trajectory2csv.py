import gc
import glob
import geopandas as gpd

import os
import logging
import json
import pandas as pd
from time import time
from functools import wraps
from google.cloud import bigquery
from google.oauth2 import service_account
from shapely import wkt
import sys
import uuid
import westernsalt_config as wc

def divide_chunks(rows_list, size=5000):
        """
        bigquery의 Maximum rows per request: 10,000 rows per request 임으로, size 만큼씩 잘라서 bigquery에 적재해야 한다.
        """
        for i in range(0, len(rows_list), size):
            yield rows_list[i : i + size]

def flush_into_bigquery():
        """
        rows_to_insert_dict에 logging된 데이터를 빅쿼리에 적재합니다.
        """
        for i in rows_to_insert_dict:
            list_of_insert_rows_list = list(
                divide_chunks(rows_to_insert_dict[i])
            )
            for j in range(0, len(list_of_insert_rows_list)):
                insert_rows_list = list_of_insert_rows_list[j]
                table = bigquery_client.get_table(i)  # Make an API request.

                df = pd.DataFrame(insert_rows_list)

                json_rows = [json.loads(i) for i in df.apply(lambda x: x.to_json(), axis=1)]

                errors = bigquery_client.insert_rows_json(
                    table, json_rows
                )

                #)  # Make an API request.
                if errors != []:
                    for error in errors:
                        print(error)

"""
REGION_NUMBER_LIST = [11000, 41000, 28000] # 서울, 경기, 인천
REGION_NAME_LIST = ["seoul", "gyeonggi", "incheon"]
CREDENTIALS = service_account.Credentials.from_service_account_file(ROOT_PATH + "/dispatching_system/tada-data-analytics-python.json")
LOG_FORMAT = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# 로깅을 위한 빅쿼리 테이블을 지정. 사용 이전에 테이블 생성 필수.
# 테이블 생성은 첨부 파일 확인
LOGGING_ID = "kirby"
"""

ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "../"))
bigquery_client = bigquery.Client(
                credentials=wc.CREDENTIALS, project=wc.CREDENTIALS.project_id
            )

sys.setrecursionlimit(3000)

uuid_list = list()
start_time = time()  # 시작 시간 저장

for i in range(len(wc.REGION_NUMBER_LIST)):
    REGION_NUMBER = wc.REGION_NUMBER_LIST[i]
    REGION_NAME = wc.REGION_NAME_LIST[i]

    FILE_PATH = ROOT_PATH + f"/dispatching_system/geodata/Z_KAIS_TL_SPRD_MANAGE_{REGION_NUMBER}.shp"

    # CSV로 저장할 파일명
    FILE_NAME = f"{REGION_NAME}_latlan.csv" # 28이 인천

    if os.path.isfile(ROOT_PATH + f"/dispatching_system/geodata/{FILE_NAME}") :
        gdf = pd.read_csv(ROOT_PATH + f"/dispatching_system/geodata/{FILE_NAME}")
        # gdf = gdf.to_crs({"init": "epsg:4326"})
    else:
        gdf = gpd.GeoDataFrame.from_file(FILE_PATH, encoding='CP949')
        gdf = gdf.to_crs({"init": "epsg:4326"})
        gdf.to_csv(ROOT_PATH + f"/dispatching_system/geodata/{FILE_NAME}", encoding='utf8', index=False)

    # Shape 파일을 GeoDataFrame으로 불러오기
    print(FILE_NAME, REGION_NAME, REGION_NUMBER, " : ", time() - start_time)

    gdf['geometry'] = gdf['geometry'].apply(wkt.loads)
    # gdf = gdf.explode() # 멀티 스트링 해제

    rows_to_insert_dict = {
                wc.LOGGING_PATH + f".{REGION_NAME}_trajectory": []
            }
    table_id = wc.LOGGING_PATH + f".{REGION_NAME}_trajectory"

    print("Data lat, lng process start : ", time() - start_time)

    for j in range(len(gdf)):
        temp_row = gdf.iloc[j]
        add_row = dict()
        add_row["multi_location"] = False

        # uuid 생성
        unique_id = uuid.uuid4()
        unique_id = int(str(unique_id.int)[:12])

        add_row["uuid"] = unique_id

        try:
            list_point = list(temp_row["geometry"].coords)

            add_row["start_lat"] = list_point[0][0]
            add_row["start_lng"] = list_point[0][1]
            add_row["end_lat"] = list_point[1][0]
            add_row["end_lng"] = list_point[1][1]
        except:
            add_row["multi_location"] = True
            add_row["multi_latlng"] = str(temp_row["geometry"])

        add_row["road_name"] = temp_row["RN"]
        add_row["rbp_cn"] = temp_row["RBP_CN"]
        add_row["rep_cn"] = temp_row["REP_CN"]

        rows_to_insert_dict[table_id].append(add_row)
        # bigquery에 넣을 데이터 logging

    print("Data lat, lng flushing start",  " : ", time() - start_time)
    flush_into_bigquery()
    
    # 메모리 정리
    del gdf
    gc.collect()

print("done", " : ", time() - start_time)