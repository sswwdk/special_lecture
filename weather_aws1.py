import mariadb
import sys
import requests  # URL 호출을 위한 라이브러리
from datetime import datetime

# 1. 데이터를 가져올 API URL (실제 URL로 반드시 변경해주세요)
api_url = "https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?stn=0&disp=0&help=1&authKey=O88XDQBsQVWPFw0AbPFVQg"

# 2. API를 호출하여 데이터 받아오기
try:
    response = requests.get(api_url)
    # HTTP 요청이 성공했는지 확인 (상태 코드가 200이 아니면 에러 발생)
    response.raise_for_status() 
    # 응답받은 내용을 텍스트 데이터로 변환
    api_response_data = response.text
    print("API 데이터 수신 성공!")

except requests.exceptions.RequestException as e:
    print(f"API 호출 중 오류 발생: {e}")
    sys.exit(1)


# 3. MariaDB 연결 정보 (사용자 환경에 맞게 수정)
try:
    conn = mariadb.connect(
        user="lguplus7",      # DB 사용자 계정
        password="lg7p@ssw0rd~!", # DB 비밀번호
        host="localhost",         # DB 호스트 (IP 또는 도메인)
        port=3310,                # DB 포트
        database="cp_data"        # 사용할 데이터베이스 이름
    )
except mariadb.Error as e:
    print(f"MariaDB 연결 오류: {e}")
    sys.exit(1)

# DB 작업을 위한 커서 생성
cur = conn.cursor()

# 삽입/조회 성공 및 스킵 카운트
insert_count = 0
skip_count = 0

# 4. 받아온 데이터를 한 줄씩 읽어와서 처리
lines = api_response_data.strip().splitlines()

for line in lines:
    clean_line = line.strip()

    if not clean_line or not clean_line[0].isdigit():
        continue

    parts = clean_line.split()
    
    if len(parts) != 18:
        print(f"데이터 형식 오류, 건너뜀: {clean_line}")
        continue

    yyyymmddhhmi = parts[0]
    stn = parts[1]

    # 5. 중복 데이터 검사
    try:
        cur.execute(
            "SELECT seq_no FROM tb_weather_aws1 WHERE yyyymmddhhmi = ? AND stn = ?",
            (yyyymmddhhmi, stn)
        )
        if cur.fetchone() is not None:
            skip_count += 1
            continue
            
    except mariadb.Error as e:
        print(f"데이터 조회 중 오류 발생: {e}")
        continue

    # 6. 데이터 삽입
    try:
        update_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        insert_data = tuple(parts) + (clean_line, update_time)
        
        cur.execute(
            """
            INSERT INTO tb_weather_aws1 (
                yyyymmddhhmi, stn, wd1, ws1, wds, wss, wd10, ws10, ta, re,
                rn_15m, rn_60m, rn_12h, rn_day, hm, pa, ps, td,
                org_data, update_dt
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            insert_data
        )
        insert_count += 1
        
    except mariadb.Error as e:
        print(f"데이터 삽입 중 오류 발생: {e}")
        conn.rollback()

# 7. 모든 작업 완료 후, 변경사항을 DB에 최종 반영
conn.commit()
print(f"작업 완료! 총 {insert_count} 건 삽입, {skip_count} 건 건너뜀.")

# 8. DB 연결 종료
cur.close()
conn.close()