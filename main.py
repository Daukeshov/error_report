from download_data_atoll import df_atoll1
from download_data_soem import df_soem
from download_data_nce import mw_nce
from download_data_tr_map import tr_map_links
import pandas as pd


# Выгрузка актуальной даты и ее преобразование
# def date():
#     date_now = str(datetime.datetime.now())
#     date_ftp = date_now[:-16]  # удаляет последние 16 символов
#     date_u2000 = date_ftp.replace('-', '_')  # дата для CSV файла U2000
#     date_soem = date_ftp.replace('-', '')  # дата для CSV файла SOEM16

#Сравнение и репорт по ошибкам частот
def error_freq():
    df_freq1 = pd.concat([df_soem, df_atoll1, mw_nce], axis=0)
    df_freq2 = df_freq1.drop_duplicates(keep=False)
    df_freq3 = df_freq2.reset_index(drop=True)
    name_status_error = df_freq3[["NAME"]]
    ID = name_status_error['NAME'].str[:2]
    df_freq3['Reg ID'] = ID
    df_freq4 = df_freq3.reindex(columns=['Reg ID', 'NAME', 'FREQ_A', 'FREQ_B'])
    writer = pd.ExcelWriter('Report_ATOLL_LINK.xlsx', engine='xlsxwriter')
    df_freq4.to_excel(writer, 'Freq_error')
    writer.save()


#Сравнение и репорт по ошибкам
def error_link(tr_map, atoll):
    df_error = pd.concat([tr_map_links, df_atoll1])
    df_sr1 = df_error.drop_duplicates(subset=['NAME'], keep=False)
    all_link4 = df_sr1.reset_index(drop=True)
    name_status_error = all_link4[["NAME"]]
    ID = name_status_error['NAME'].str[:2]
    name_status_error['Reg ID'] = ID
    name_status_error = name_status_error.reindex(columns=['Reg ID', 'NAME'])
    # print(name_status_error)
    # writer = pd.ExcelWriter('Report_ATOLL_LINK.xlsx', engine='xlsxwriter')
    # name_status_error.to_excel(writer, 'NAME_COMMENT_STATUS')
    # writer.save()


if __name__ == '__main__':
    error_link()
    error_link()
    error_freq()

