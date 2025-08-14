import mariadb
import sys
import requests
import time
from datetime import datetime

# DB 연결 함수
def get_db_connection():
    try:
        conn = mariadb.connect(
            user="lguplus7",
            password="lg7p@ssw0rd~!",
            host="localhost",
            port=3310,
            database="cp_data"
        )
        return conn
    except mariadb.Error as e:
        print(f"MariaDB 연결 오류: {e}")
        sys.exit(1)

# 데이터 처리 함수
def fetch_and_store():
    # 현재 시각을 tm2 파라미터에 맞게 포맷
    tm2 = datetime.now().strftime("%Y%m%d%H%M")
    
    api_url = f"https://apihub.kma.go.kr/api/typ01/cgi-bin/url/nph-aws2_min?stn=0&disp=0&help=1&authKey=O88XDQBsQVWPFw0AbPFVQg"

    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        api_response_data = response.text
        print(f"[{tm2}] API 데이터 수신 성공!")
    except requests.exceptions.RequestException as e:
        print(f"API 호출 중 오류 발생: {e}")
        return

    conn = get_db_connection()
    cur = conn.cursor()

    insert_count = 0
    skip_count = 0

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

    conn.commit()
    cur.close()
    conn.close()

    print(f"[{tm2}] 작업 완료! 총 {insert_count} 건 삽입, {skip_count} 건 건너뜀.")

# 매분 실행 루프
if __name__ == "__main__":
    while True:
        fetch_and_store()
        time.sleep(60)
