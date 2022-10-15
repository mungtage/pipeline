import os
import math
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pytz import timezone

def get_url(API_Key, date, page_number = 1):
    URL = f"http://apis.data.go.kr/1543061/abandonmentPublicSrvc/abandonmentPublic?bgnde={date}&numOfRows=1000&pageNo={page_number}&serviceKey={API_Key}&_type=json"
    return URL

def get_requests_params(key, days_ago):
    API_Key = os.environ.get(key)
    print(API_Key)
    date = (datetime.now(timezone('Asia/Seoul'))- timedelta(days_ago)).strftime('%Y%m%d')
    return API_Key, date

def get_total_count_pages(API_Key, date):
    URL = get_url(API_Key, date)
    rq = requests.get(URL)
    data = rq.json()
    animal_info_totalCount = data['response']['body']['totalCount']
    animal_info_totalPages = math.ceil(animal_info_totalCount/1000)
    return animal_info_totalCount, animal_info_totalPages

def get_info_list_by_page(API_Key, date, page_number):
    URL = get_url(API_Key, date, page_number)
    rq = requests.get(URL)
    data = rq.json()
    info_list = data['response']['body']['items']['item']
    return info_list

load_dotenv()
API_Key, date = get_requests_params("ApiKey", 10)
print(API_Key)
animal_info_totalCount, animal_info_totalPages = get_total_count_pages(API_Key, date)

for page_number in range(1, animal_info_totalPages+1):
    for idx, info_dict in enumerate(tqdm(get_info_list_by_page(API_Key, date, page_number))):
        print(info_dict)
