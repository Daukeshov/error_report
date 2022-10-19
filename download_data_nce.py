from ftplib import FTP
import pandas as pd

# Подключение к FTP серверу и выгрузка данных (пролет и частоты) в DataFrame
def ftp_nce():
    # Подключение к FTP серверу SOEM и выгрузка данных в DataFrame
    def ftp_connect():
        ftp = FTP('8.8.8.8')  # IP ftp
        ftp.login('user', 'password')
        ftp.cwd('U2000')
        files_list1 = ftp.nlst()
        df_list = pd.DataFrame(files_list1, columns=["A"])
        file_name = df_list.iloc[-1, :]['A'] # сортировка и выбор последней строки
        ftp.cwd(file_name)
        files_list2 = ftp.nlst()
        df_list2 = pd.DataFrame(files_list2, columns=["A"])
        df_nce = df_list2[df_list2['A'].str.contains('Microwave_Link_Report_')]
        nce = df_nce.iloc[-1, :]['A'] # сортировка и выбор последней строки
        ftp.retrbinary("RETR " + nce, open(nce, 'wb').write)
        global df_all
        df_all = pd.read_csv(nce, skiprows=11)
        ftp.quit()


    #Select столбцов и формирование пролета с частотами
    def data_nce():
        df_all.replace(regex=[r"MW."], value="", inplace=True)
        df_all.rename(columns={'Source NE Frequency (MHz)': 'FREQ_A', 'Sink NE Frequency (MHz)': 'FREQ_B',
                               'Source NE Name': 'Source_NE_Name', 'Sink NE Name': 'Sink_NE_Name'}, inplace=True)
        df_nce = df_all[["Source_NE_Name", "Sink_NE_Name", "FREQ_A", "FREQ_B"]]
        global mw_nce
        mw_nce = df_nce.assign(NAME=df_nce.Source_NE_Name.astype(str) + ' - ' + df_nce.Sink_NE_Name.astype(str))
        mw_nce = mw_nce[["NAME", "FREQ_A", "FREQ_B"]]

