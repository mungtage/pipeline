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
from utils.querys import make_query_insert, make_query_truncate

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

def get_data():
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
    
def image_download(df):
    IMG_PATH = "./images/announcement"
    os.makedirs(IMG_PATH, exist_ok=True)
    previous_images = set([os.path.splitext(os.path.basename(path))[0] for path in os.listdir(IMG_PATH)])
    current_images = set(df["desertionNo"].to_list())
    target_images = current_images - previous_images
    
    current_images_json = df[df["desertionNo"].isin(target_images)][["desertionNo","popfile"]].to_dict("records")

    for item_dict in tqdm(current_images_json):
        responds = requests.get(item_dict['popfile'])
        open(f"./images/{item_dict['desertionNo']}.jpg", "wb").write(responds.content)
    
    
def main():
    start = time.time()
    
    data = get_data()
    df = preprocess_data(data)
    
    query_insert = make_query_insert(df.columns.to_list())
    result_data = df.to_dict('records')
    post_data(query_insert, result_data)
    
    image_download(df)

    total_elapsed_time = f"total elapsed time : {time.time() - start}\n" 
    print(total_elapsed_time)
if __name__ == "__main__":
    main()
