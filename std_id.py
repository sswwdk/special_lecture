from time import sleep
import mariadb
import sys
import requests

try:
    conn_tar = mariadb.connect(
        user="lguplus7",
        password="lg7p@ssw0rd~!",
        host="localhost",
        port=3310,
        database="cp_data"
    )
except mariadb.Error as e:
    print(f"Error connecting to MariaDB Platform: {e}")
    sys.exit(1)

tar_cur = conn_tar.cursor()

req_url = 'https://apihub.kma.go.kr/api/typ01/url/stn_inf.php?inf=AWS&stn=&tm=202211300900&help=1&authKey=O88XDQBsQVWPFw0AbPFVQg'

while True:
    response = requests.get(req_url)
    org_data = response.text

    split_data = org_data.strip().replace('    ',' ').replace('   ',' ').replace('  ',' ').split('\n')
    for line in split_data:
        if line.startswith('#'):
            continue

        print(line)
        line_arr = line.strip().split(' ')

        # 예시: API 데이터 항목 추출 (시간, 지점번호 등)
        yyyymmddhhmi = line_arr[0]  # 날짜시간 (create_dt에 활용)
        STN_ID = line_arr[1]        # 지점번호

        # 나머지는 현재 테이블 구조에 맞게 데이터가 없어 기본값 또는 빈 문자열로 처리
        LON = '0'
        LAT = '0'
        STN_SP = '0'
        HT = '0'
        HT_WD = '0'
        LAU_ID = '0'  # 쿼리에 'LAU_ID'로 되어있음 (원래는 'LAW_ID'인지 확인 필요)
        STN_AD = '0'
        STN_KO = '0'
        STN_EN = '0'
        FCT_ID = '0'
        LAW_ID = '0'
        BASIN = '0'
        addr1 = '0'
        addr2 = '0'
        addr3 = '0'
        org_addr = org_data
        create_dt = yyyymmddhhmi

        # 중복 체크 (기존엔 tb_weather_aws1 기준인데, 새 테이블로 바꿈)
        tar_cur.execute("SELECT seq_no FROM tb_weather_tcn WHERE STN_ID = ? AND create_dt = ?", (STN_ID, create_dt))
        exist_list = tar_cur.fetchall()

        if exist_list and len(exist_list) > 0:
            print(f'[debug] duplicated : STN_ID={STN_ID}, create_dt={create_dt}')
        else:
            tar_cur.execute(
                """INSERT INTO tb_weather_tcn
                (STN_ID, LON, LAT, STN_SP, HT, HT_WD, LAU_ID, STN_AD, STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN,
                 addr1, addr2, addr3, org_addr, create_dt)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (STN_ID, LON, LAT, STN_SP, HT, HT_WD, LAU_ID, STN_AD, STN_KO, STN_EN, FCT_ID, LAW_ID, BASIN,
                 addr1, addr2, addr3, org_addr, create_dt)
            )
            conn_tar.commit()
            print('insert into tb_weather_tcn done')

    sleep(60)