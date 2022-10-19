import os
from dotenv import load_dotenv
import pandas as pd

# Подключение к серверу Atoll и выгрузка данных
def data_atoll():
    # Подключение к БД Atoll и выгрузка данных MW_Links
    def atoll_connect():
        load_dotenv()
        query = "SELECT * FROM ATOLL_MW.dbo.MWLinks"
        con = os.environ.get('DATABASE_URL')
        global df_links
        df_links = pd.read_sql(query, con)
        df_links.replace(regex=[r"_.*"], value="", inplace=True)

    # Select нужных столбцов  и удаление пролетов где статус "Not exist"
    def atoll_select():
        df_atoll = df_links[["NAME", "FREQ_A", "FREQ_B", "COMMENT_"]]
        active = df_atoll[df_atoll.COMMENT_ != 'Not exist']
        global df_atoll1
        df_atoll1 = active[["NAME", "FREQ_A", "FREQ_B"]]

