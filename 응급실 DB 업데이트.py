# 응급실 프로젝트 DB 업데이트 코드

import pymysql
import json
import requests
import xmltodict
import warnings
from bs4 import BeautifulSoup as bs
import schedule

conn = None
cur = None

conn = pymysql.connect(host='127.0.0.1', port=3306, user='', password='', db='', charset='utf8')
cur = conn.cursor()

# 지역 이름
localName = {
    '서울특별시' : ["강남구", "강동구", "강서구", "광진구", "구로구", "금천구", "노원구", "도봉구", "동대문구", "동작구", "서대문구", "서초구", "성동구", "성북구", "송파구", "양천구", "영등포구", "용산구", "은평구", "종로구", "중구", "중랑구"],
    '인천광역시' : ["계양구", "남동구", "동구", "미추홀구", "부평구", "서구", "연수구", "중구"],
    '부산광역시' : ["남구", "동구", "동래구", "부산진구", "북구", "사상구", "서구", "수영구", "연제구", "영도구", "중구", "해운대구"],
    '대전광역시' : ["대덕구", "동구", "서구", "유성구", "중구"],
    '대구광역시' : ["남구", "달서구", "동구", "북구", "서구", "수성구", "중구"],
    '울산광역시' : ["남구", "동구", "북구", "중구", "울주군"],
    '광주광역시' : ["광산구", "남구", "동구", "북구", "서구"],
    '제주특별자치도' : ["서귀포시", "제주시"],
    '세종특별자치시' : ["세종시"],
    '경기도' : ["고양시", "광명시", "광주시", "구리시", "군포시", "김포시", "남양주시", "부천시", "성남시", "수원시", "시흥시", "안산시", "안성시", "안양시", "양주시", "여주시", "오산시", "용인시", "의왕시", "의정부시", "이천시", "파주시", "평택시", "포천시", "화성시", "양평군", "연천군"],
    '강원도' : ["강릉시", "동해시", "삼척시", "속초시", "원주시", "춘천시", "태백시", "양구군", "영월군", "정선군", "철원군", "평창군", "홍천군", "화천군", "횡성군"],
    '충청북도' : ["제천시", "청주시", "충주시", "괴산군", "영동군", "옥천군", "진천군"],
    '충청남도' : ["공주시", "논산시", "당진시", "보령시", "서산시", "아산시", "천안시", "부여군", "서천군", "청양군", "태안군", "홍성군"],
    '경상북도' : ["경산시", "경주시", "구미시", "김천시", "문경시", "상주시", "안동시", "영주시", "영천시", "포항시", "고령군", "성주군", "예천군", "울릉군", "울진군", "의성군", "청도군", "청송군"],
    '경상남도' : ["거제시", "김해시", "밀양시", "사천시", "양산시", "진주시", "창원시", "통영시", "거창군", "고성군", "남해군", "산청군", "의령군", "창녕군", "함양군", "합천군"],
    '전라북도' : ["군산시", "김제시", "남원시", "익산시", "전주시", "정읍시", "고창군", "무주군", "부안군", "순창군", "임실군", "장수군", "진안군"],
    '전라남도' : ["광양시", "나주시", "목포시", "순천시", "여수시", "강진군", "고흥군", "곡성군", "구례군", "담양군", "무안군", "보성군", "신안군", "영광군", "영암군", "완도군", "장성군", "장흥군", "진도군", "해남군", "화순군"]
}

update_col = ["hvidate", "hvec", "hvgc", "hvctayn", "hvmriayn", "hv28", "hv27", "hv29", "hv30",
              "hvcc", "hvncc", "hvccc", "hvicc", "hv2", "hv3", "hv6", "hv7", "hv8", "hv9", "hvoc"]

url = 'http://apis.data.go.kr/B552657/ErmctInfoInqireService/getEmrrmRltmUsefulSckbdInfoInqire'

def fetch_data(params):
    try:
        resp = requests.get(url, params=params)
        if resp.status_code == 200:
            data_xml = bs(resp.text, 'html.parser')
            xml_string = str(data_xml)
            data_json = json.loads(json.dumps(xmltodict.parse(xml_string), indent=4))
            return data_json.get('response').get('body').get('items').get('item')
        return None
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None

def update_database(data_list):
    try:
        if isinstance(data_list, dict):
            data_list = [data_list]
        
        for item in data_list:
            insert_value = ", ".join(
                [f"{col}_ST = '{item.get(col, '-')}'" for col in update_col]
            )
            sql = f"UPDATE er_avail_tb SET {insert_value} WHERE hpid_ST = '{item.get('hpid')}'"
            cur.execute(sql)
            conn.commit()
    except Exception as e:
        print(f"Error updating database: {e}")

def update_func():
    fail = False
    for local in localName:
        for city in localName.get(local):
            params = {
                'serviceKey': '',
                'STAGE1': local,
                'STAGE2': city,
                'pageNo': '1',
                'numOfRows': '10'
            }

            data_list = fetch_data(params)
            
            if data_list:
                update_database(data_list)
                fail = False
            else:
                fail = True

            if fail:
                previous_city_index = localName.get(local).index(city) - 1
                if previous_city_index >= 0:
                    city = localName.get(local)[previous_city_index]
                    params['STAGE2'] = city
                    data_list = fetch_data(params)
                    
                    if data_list:
                        update_database(data_list)
                        fail = False
                    else:
                        fail = True

    return "완료"

schedule.every(8).minutes.do(update_func)
