import mariadb
import sys

# --- 데이터베이스 접속 정보 ---
# 보안을 위해 실제 코드에서는 직접 작성하는 대신, 환경 변수나 설정 파일을 사용하는 것이 좋습니다.
SRC_DB_CONFIG = {
    "user": "lguplus7",
    "password": "lg7p@ssw0rd~!",
    "host": "192.168.14.38",
    "port": 3310,
    "database": "cp_data"
}

TARGET_DB_CONFIG = {
    "user": "lguplus7",
    "password": "lg7p@ssw0rd~!",
    "host": "localhost",
    "port": 3310,
    "database": "cp_data"
}

# --- 동기화할 컬럼 목록 ---
# seq_no는 AUTO_INCREMENT이므로 제외합니다.
COLUMNS_TO_SYNC = [
    "STN_ID", "LON", "LAT", "STN_SP", "HT", "HT_WD", "LAU_ID", "STN_AD",
    "STN_KO", "STN_EN", "FCT_ID", "LAW_ID", "BASIN", "addr1", "addr2",
    "addr3", "org_addr", "create_dt"
]

# --- 데이터베이스 연결 ---
try:
    conn_src = mariadb.connect(**SRC_DB_CONFIG)
    conn_tar = mariadb.connect(**TARGET_DB_CONFIG)
except mariadb.Error as e:
    print(f"데이터베이스 연결 오류: {e}")
    sys.exit(1)

# with 구문을 사용하여 작업 후 커서가 자동으로 정리되도록 합니다.
with conn_src.cursor() as src_cur, conn_tar.cursor() as tar_cur:
    try:
        # 1. 원본(Source) DB에서 모든 데이터 가져오기
        # 컬럼 순서를 보장하기 위해 SELECT * 대신 컬럼명을 명시합니다.
        select_query = f"SELECT {', '.join(COLUMNS_TO_SYNC)} FROM tb_weather_tcn"
        src_cur.execute(select_query)
        source_records = src_cur.fetchall()
        print(f"원본 DB에서 {len(source_records)}개의 레코드를 가져왔습니다.")

        insert_count = 0
        update_count = 0

        for record in source_records:
            # record는 튜플 형태이므로, 딕셔너리로 변환하여 다루기 쉽게 만듭니다.
            record_dict = dict(zip(COLUMNS_TO_SYNC, record))
            stn_id = record_dict.get("STN_ID")

            # 2. 대상(Target) DB에 STN_ID 기준으로 데이터가 있는지 확인
            tar_cur.execute("SELECT STN_ID FROM tb_weather_tcn WHERE STN_ID = ?", (stn_id,))
            
            if tar_cur.fetchone() is None:
                # 2-1. 데이터가 없으면 INSERT
                # 플레이스홀더(?)를 사용하여 SQL Injection 공격을 방지합니다.
                cols = ', '.join(record_dict.keys())
                placeholders = ', '.join(['?'] * len(record_dict))
                insert_sql = f"INSERT INTO tb_weather_tcn ({cols}) VALUES ({placeholders})"
                
                tar_cur.execute(insert_sql, list(record_dict.values()))
                insert_count += 1
                print(f"  -> [INSERT] STN_ID: {stn_id} (신규 데이터)")
            else:
                # 2-2. (선택사항) 데이터가 있으면 UPDATE
                # 여기서는 기존 데이터를 업데이트하는 로직을 추가할 수 있습니다.
                # 예: create_dt를 비교하여 최신 데이터로 업데이트
                # 이 예제에서는 간단히 건너뛰지만, 필요시 아래 주석을 해제하여 사용하세요.
                # print(f"  -> [SKIP] STN_ID: {stn_id} (이미 존재함)")
                pass

        # 3. 모든 작업이 성공적으로 끝나면 변경사항을 최종 반영(commit)
        if insert_count > 0 or update_count > 0:
            conn_tar.commit()
            print(f"\n성공적으로 동기화를 완료했습니다.")
            print(f"  - 신규 추가: {insert_count} 건")
            print(f"  - 업데이트: {update_count} 건")
        else:
            print("\n새롭게 추가하거나 업데이트할 데이터가 없습니다.")

    except mariadb.Error as e:
        # 4. 작업 중 오류 발생 시 모든 변경사항을 취소(rollback)
        print(f"\n작업 중 오류가 발생했습니다: {e}")
        print("모든 변경사항을 롤백합니다.")
        conn_tar.rollback()

    finally:
        # 5. 데이터베이스 연결 종료
        conn_src.close()
        conn_tar.close()
        print("데이터베이스 연결을 모두 종료했습니다.")