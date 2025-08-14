# --- 1. 라이브러리 가져오기 (모듈 임포트) ---
# os: 'operating system'의 약자로, 폴더 안의 파일 목록을 가져오는 등 운영체제 기능을 사용하기 위해 필요합니다.
import os
# csv: CSV(Comma-Separated Values) 파일을 쉽게 읽고 쓸 수 있게 도와주는 라이브러리입니다.
import csv
# mariadb: MariaDB 데이터베이스에 연결하고 SQL 명령을 실행하기 위한 라이브러리입니다.
import mariadb
# sys: 'system'의 약자로, 프로그램 실행을 중단시키는(sys.exit) 등 시스템 관련 기능을 사용하기 위해 필요합니다.
import sys

# --- 2. 설정 정보 변수화 (유지보수를 쉽게 만들기 위함) ---
# 왜 이렇게 할까요? -> 나중에 DB 정보나 파일 경로가 바뀌었을 때, 코드 전체를 헤맬 필요 없이 이 부분만 수정하면 되기 때문입니다.
# --------------------------------------------------------------------
# 데이터베이스 연결 정보
DB_USER = "lguplus7"
DB_PASSWORD = "lg7p@ssw0rd~!"
DB_HOST = "localhost"
DB_PORT = 3310
DB_NAME = "cp_data"
TABLE_NAME = "tb_smb_ods"

# CSV 파일들이 저장된 폴더 경로
CSV_DIRECTORY = "C:/data/smb_data"


def load_csvs_to_mariadb_direct():
    """
    지정된 폴더의 모든 CSV 파일을 찾아서 MariaDB 테이블에 적재하는 함수.
    이처럼 기능 전체를 함수로 묶어두면 나중에 다른 파이썬 파일에서 이 기능을 쉽게 불러와 재사용할 수 있습니다.
    """
    print("CSV -> MariaDB 데이터 적재 프로세스를 시작합니다...")

    # --- 3. 데이터베이스 연결 (가장 기본적인 단계) ---
    # 왜 try...except를 쓸까요? -> DB 비밀번호가 틀리거나, DB 서버가 꺼져있는 등 연결에 실패할 수 있기 때문입니다.
    # 이런 예외 상황이 발생했을 때 프로그램이 그냥 멈추지 않고, "연결 실패" 메시지를 보여주고 안전하게 종료시키기 위함입니다.
    # --------------------------------------------------------------------
    try:
        # conn 변수에 데이터베이스 연결 객체를 저장합니다.
        conn = mariadb.connect(
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME
        )
        print("MariaDB에 성공적으로 연결되었습니다.")
    except mariadb.Error as e:
        # mariadb 라이브러리에서 오류가 발생하면 그 내용을 e 변수에 담습니다.
        print(f"MariaDB 연결 오류: {e}")
        sys.exit(1) # 프로그램 강제 종료

    # SQL 쿼리를 실행하기 위한 커서(cursor) 객체를 생성합니다.
    # 커서는 DB에 명령을 내리고 결과를 받아오는 역할을 하는 포인터 같은 것입니다.
    cur = conn.cursor()

    # --- 4. SQL INSERT 문 준비 (효율적인 반복 작업을 위함) ---
    # 왜 이렇게 할까요? -> 39개의 값을 일일이 SQL 문에 넣는 코드를 반복문 안에서 계속 만드는 것은 비효율적입니다.
    # 물음표(?)를 사용한 '틀'을 미리 만들어두고, 반복문 안에서는 값만 바꿔 끼워넣어 속도를 높이고 코드를 깔끔하게 유지합니다.
    # --------------------------------------------------------------------
    placeholders = ", ".join(["?"] * 39) # 물음표(?) 39개를 콤마(,)로 연결하여 문자열 생성 -> "?, ?, ..., ?"
    insert_sql = f"INSERT INTO {TABLE_NAME} VALUES ({placeholders})"
    print(f"사용될 SQL 템플릿: {insert_sql}")

    # --- 5. CSV 파일 처리 및 데이터 적재 (핵심 로직) ---
    # 왜 try...except...finally를 쓸까요? -> 파일 처리 중에는 다양한 오류가 발생할 수 있습니다.
    # e.g.) 폴더 경로가 잘못되었을 때 (FileNotFoundError), 파일은 있지만 읽기 권한이 없을 때, 파일 내용이 깨져있을 때 등
    # 이런 오류가 발생해도 다른 파일 처리는 계속하거나, 최소한 DB 연결은 안전하게 닫아주기 위해 사용합니다.
    # --------------------------------------------------------------------
    try:
        # 지정된 폴더(CSV_DIRECTORY) 안의 모든 파일 및 폴더 목록을 가져옵니다.
        files_in_directory = os.listdir(CSV_DIRECTORY)
        # 그중에서 파일 이름이 '.csv'로 끝나는 파일들만 골라서 리스트로 만듭니다.
        csv_files = [f for f in files_in_directory if f.endswith('.csv')]

        # 만약 CSV 파일이 하나도 없다면, 사용자에게 알려주고 함수를 종료합니다.
        if not csv_files:
            print(f"폴더 '{CSV_DIRECTORY}'에서 CSV 파일을 찾을 수 없습니다.")
            return

        print(f"총 {len(csv_files)}개의 CSV 파일을 처리합니다.")

        # 찾은 CSV 파일 목록을 하나씩 반복 처리합니다.
        for filename in csv_files:
            # 파일 경로와 파일 이름을 합쳐서 전체 파일 경로를 만듭니다. (e.g., "C:/data/sbo_data/seoul.csv")
            file_path = os.path.join(CSV_DIRECTORY, filename)
            print(f"파일 처리 중: {file_path}...")
            
            # 각 파일별로도 오류가 발생할 수 있으므로, 여기서도 try...except로 감싸줍니다.
            # 이렇게 하면 특정 파일 하나에 문제가 생겨도 전체 프로그램이 멈추지 않고 다음 파일 처리를 계속할 수 있습니다.
            try:
                # 'with open(...)' 구문: 파일을 열고, 코드 블록이 끝나면 자동으로 파일을 닫아줍니다. (conn.close()처럼 중요)
                # encoding='utf-8': 한글이 깨지지 않도록 인코딩 방식을 지정합니다.
                with open(file_path, mode='r', encoding='utf-8') as csvfile:
                    # CSV 파일을 읽는 객체를 생성합니다.
                    csv_reader = csv.reader(csvfile)
                    
                    # 헤더(첫 번째 줄)를 건너뛰어 데이터로 입력되는 것을 방지합니다.
                    next(csv_reader, None)
                    
                    rows_inserted = 0
                    # CSV 파일의 데이터를 한 줄(row)씩 반복해서 읽어옵니다.
                    for row in csv_reader:
                        # 데이터의 컬럼 수가 39개가 아닐 경우, 경고 메시지를 출력하고 해당 줄은 건너뜁니다. (데이터 무결성 유지)
                        if len(row) == 39:
                            cur.execute(insert_sql, tuple(row)) # 준비된 SQL 틀에 실제 데이터(row)를 넣어 실행
                            rows_inserted += 1
                        else:
                            print(f"  [경고] {filename} 파일의 행(row)이 39개가 아니어서 건너뜁니다: {row}")
                
                # 파일 하나를 성공적으로 다 읽었으면, 지금까지의 변경사항을 데이터베이스에 최종 반영(커밋)합니다.
                # 한 줄마다 커밋하는 것보다 파일 하나를 끝내고 커밋하는 것이 훨씬 효율적입니다.
                conn.commit()
                print(f"'{filename}' 파일의 {rows_inserted}개 행을 성공적으로 적재하고 커밋했습니다.")

            except Exception as e:
                print(f"  [오류] '{filename}' 파일 처리 중 오류 발생: {e}")
                print("  이 파일을 건너뛰고 다음 파일 처리를 계속합니다.")

        print("\n모든 CSV 파일 처리가 완료되었습니다.")

    except FileNotFoundError:
        print(f"오류: 폴더 '{CSV_DIRECTORY}'를 찾을 수 없습니다. 경로를 확인해주세요.")
    except Exception as e:
        print(f"예상치 못한 오류가 발생했습니다: {e}")
    finally:
        # --- 6. 자원 해제 (마무리 작업) ---
        # 왜 finally를 쓸까요? -> try 블록에서 오류가 발생하든 안 하든, 이 부분은 '항상' 실행됩니다.
        # 프로그램이 중간에 오류로 멈추더라도 데이터베이스 연결은 반드시 닫아줘야 하기 때문에 finally 구문을 사용합니다.
        # 연결을 닫지 않으면 불필요한 연결이 계속 쌓여서 DB 서버에 부하를 줄 수 있습니다.
        # --------------------------------------------------------------------
        if 'conn' in locals() and conn:
            conn.close()
            print("데이터베이스 연결을 닫았습니다.")


# --- 7. 프로그램 실행 ---
# 이 코드가 다른 파일에서 import될 때는 실행되지 않고, 직접 이 파일을 실행할 때만 아래 함수를 호출하라는 의미의 파이썬 관례입니다.
if __name__ == "__main__":
    load_csvs_to_mariadb_direct()