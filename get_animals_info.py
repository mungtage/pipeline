import os
import math
import time
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pytz import timezone
from pprint import pprint
import pymysql
import pandas as pd
from utils.querys import make_query_insert, make_query_truncate, make_query_select
from utils.image_pipeline import image_pipeline

def get_url(API_Key, date, page_number = 1):
    dog_code = 417000
    url = f"http://apis.data.go.kr/1543061/abandonmentPublicSrvc/abandonmentPublic"
    params = {
        'upkind': dog_code,
        'bgnde':date,
        'numOfRows':'1000',
        'pageNo':page_number,
        'serviceKey':API_Key,
        '_type':'json'
    }
    return url, params

def get_requests_params(key, days_ago):
    API_Key = os.environ.get(key)
    date = (datetime.now(timezone('Asia/Seoul'))- timedelta(days_ago)).strftime('%Y%m%d')
    return API_Key, date

def get_total_count_pages(API_Key, date):
    url, params = get_url(API_Key, date)
    rq = requests.get(url, params=params)
    data = rq.json()
    animal_info_totalCount = data['response']['body']['totalCount']
    animal_info_totalPages = math.ceil(animal_info_totalCount/1000)
    return animal_info_totalCount, animal_info_totalPages

def get_info_list_by_page(API_Key, date, page_number):
    url, params = get_url(API_Key, date, page_number)
    rq = requests.get(url, params=params)
    data = rq.json()
    info_list = data['response']['body']['items']['item']
    return info_list

def get_api_data():
    start = time.time()
    load_dotenv()
    API_Key, date = get_requests_params("ApiKey", 10)
    animal_info_totalCount, animal_info_totalPages = get_total_count_pages(API_Key, date)

    list_of_dict = [
        info_dict
        for page_number in range(1, animal_info_totalPages+1)
        for idx, info_dict in enumerate(tqdm(get_info_list_by_page(API_Key, date, page_number)))]
    print(f"get_data elapsed time : {time.time() - start}\n")
    return list_of_dict

def preprocess_data(data):
    start = time.time()
    df = pd.DataFrame(data)
    df = df.fillna("")
    print(f"preprocess_data elapsed time : {time.time() - start}\n")
    return df

def post_data(query_insert, data):
    start = time.time()
    db = pymysql.connect(host=os.environ.get('host'), 
                         user=os.environ.get('user'), 
                         password=os.environ.get('password'), 
                         db=os.environ.get('db'), 
                         charset='utf8')
    try:
        with db.cursor() as cursor:
            cursor.execute(make_query_truncate(os.environ.get('table')))
            cursor.executemany(query_insert, data)
            db.commit()
    finally:
        db.close()
    print(f"post_data elapsed time : {time.time() - start}\n")
    
def get_db_data():
    start = time.time()
    db = pymysql.connect(host=os.environ.get('host'), 
                         user=os.environ.get('user'), 
                         password=os.environ.get('password'), 
                         db=os.environ.get('db'), 
                         charset='utf8',
                         cursorclass=pymysql.cursors.DictCursor)
    try:
        with db.cursor() as cursor:
            cursor.execute(make_query_select())
            result = cursor.fetchall()
            print(f"get_data elapsed time : {time.time() - start}\n")
            return result
    finally:
        db.close()
    
    
    
    
def main():
    IMG_PATH = "/home/aimungtage/images/announcement"
    
    start = time.time()
    
    api_data = get_api_data()
    api_df = preprocess_data(api_data)
    
    query_insert = make_query_insert(api_df.columns.to_list())
    result_data = api_df.to_dict('records')
    
    db_df = pd.DataFrame(get_api_data())
    
    image_pipeline(api_df, db_df, IMG_PATH)
    post_data(query_insert, result_data)

    total_elapsed_time = f"total elapsed time : {time.time() - start}\n" 
    print(total_elapsed_time)
    
if __name__ == "__main__":
    main()
