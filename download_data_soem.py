from ftplib import FTP
import pandas as pd


# Подключение к FTP серверу и выгрузка данных (пролет и частоты) в DataFrame
def ftp_soem():
    # Подключение к FTP серверу SOEM и выгрузка данных в DataFrame
    def ftp_connect():
        ftp = FTP('8.8.8.8')  # IP ftp
        ftp.login('user', 'password')
        ftp.cwd('INV')
        files_list = ftp.nlst()
        df_list = pd.DataFrame(files_list, columns=["A"])
        df_doc = df_list[df_list['A'].str.contains('soem16_Config_Data_MINI-LINK_TN_MMU2B_C_')]
        soem = df_doc.iloc[-1, :]['A'] # сортировка и выбор последней строки
        ftp.retrbinary("RETR " + soem, open(soem, 'wb').write)
        global df_all
        df_all = pd.read_csv(soem)
        ftp.quit()
    ftp_connect()

    # Select столбцов и формирование пролета с частотами
    def data_soem():
        df_all.replace(regex=[r"ML-"], value="", inplace=True)
        df_all['NEAlias'] = df_all['NEAlias'].str[:-2]
        df_soem1 = df_all[["NEAlias", "Terminal_ID", "tx_freq", "rx_freq"]]
        ID = df_soem1['NEAlias'].str[:-4]
        df2 = df_soem1.assign(Terminal_ID1=ID.astype(str) + '' + df_soem1.Terminal_ID.astype(str))
        df3 = df2.assign(NAME=df2.NEAlias.astype(str) + ' - ' + df2.Terminal_ID1.astype(str))
        df4 = df3[["NAME", "tx_freq", "rx_freq"]]
        global df_soem
        df_soem = df4.rename(columns = {'tx_freq':'FREQ_A', 'rx_freq':'FREQ_B'})
    data_soem()
ftp_soem()