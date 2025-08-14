import duckdb
import os
import pandas as pd

# duckdb로 csv파일을 읽으면 에러가 발생하므로 pandas에서 먼저 읽어서 duckdb에 import

duck_con = duckdb.connect(r"c:\data\duck_smb.db")
duck_con.execute("CREATE TABLE IF NOT EXISTS tb_smb_file AS SELECT * FROM read_csv('c:/data/smb_data/sejong.csv');")
duck_con.execute("DELETE FROM tb_smb_file;")

csv_path = r'c:/data/smb_data'
file_list = os.listdir(csv_path)  # 경로 내에 파일을 모두 불러옴
csv_file_list = [file for file in file_list if file.lower().endswith('.csv')]  # csv 확장자만

for csv_file_name in csv_file_list:
    csv_file_full = f'{csv_path}/{csv_file_name}'
    print(f'csv_file_name = {csv_file_full}')
    # 필요 시 encoding='cp949' 추가
    csv_df = pd.read_csv(csv_file_full, low_memory=False)

    # ✅ 핵심: DataFrame을 DuckDB에 등록해서 SELECT * FROM csv_df가 보이게 함
    duck_con.register("csv_df", csv_df)
    duck_con.execute("INSERT INTO tb_smb_file SELECT * FROM csv_df;")
    duck_con.unregister("csv_df")

print("csv loading complete")

# ✅ 한글 컬럼명은 쌍따옴표로 감싸기
duck_con.sql("SELECT COUNT(1) AS cnt FROM tb_smb_file").show()
duck_con.sql('SELECT "건물명" FROM tb_smb_file WHERE "상가업소번호" = ? LIMIT 10',
             ['MA010120220801771448']).show()