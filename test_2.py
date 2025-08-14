import duckdb
import os
import pandas as pd

# duckdb로 csv파일을 읽으면 에러가 발생하므로 pandas에서 먼저 읽어서 duckdb에 import

duck_con = duckdb.connect(r"c:\data\duck_smb.db")


# ✅ 한글 컬럼명은 쌍따옴표로 감싸기
duck_con.sql("SELECT COUNT(1) AS cnt FROM tb_smb_file").show()
duck_con.sql('SELECT * FROM tb_smb_file LIMIT 10').show()