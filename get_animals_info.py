import os
import math
import requests
from tqdm import tqdm
from datetime import datetime, timedelta
from dotenv import load_dotenv
from pytz import timezone
from pprint import pprint
import pymysql

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
    load_dotenv()
    API_Key, date = get_requests_params("ApiKey", 10)
    animal_info_totalCount, animal_info_totalPages = get_total_count_pages(API_Key, date)

    list_of_dict = [
        info_dict
        for page_number in range(1, animal_info_totalPages+1)
        for idx, info_dict in enumerate(tqdm(get_info_list_by_page(API_Key, date, page_number)))]

    # pprint(list_of_dict)
    return list_of_dict

def post_data(data):
    db = pymysql.connect(host='101.101.210.225', user='jaeho', password='1234', db='openapi', charset='utf8')
    try:
        with db.cursor() as cursor:
            query = '''INSERT INTO animal_info (age, careAddr, careNm, careTel, chargeNm, colorCd, desertionNo, filename, happenDt, happenPlace, kindCd, neuterYn, noticeEdt, noticeNo, noticeSdt, officetel, orgNm, popfile, processState, sexCd, specialMark, weight)
                VALUES(%(age)s, %(careAddr)s, %(careNm)s, %(careTel)s, %(chargeNm)s, %(colorCd)s, %(desertionNo)s, %(filename)s, %(happenDt)s, %(happenPlace)s, %(kindCd)s, %(neuterYn)s, %(noticeEdt)s, %(noticeNo)s, %(noticeSdt)s, %(officetel)s, %(orgNm)s, %(popfile)s, %(processState)s, %(sexCd)s, %(specialMark)s, %(weight)s);'''
            cursor.executemany(query, data)
            db.commit()
    finally:
        db.close()

def main():
    data = get_data()
    post_data(data)
    
if __name__ == "__main__":
    main()
