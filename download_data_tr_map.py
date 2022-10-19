from ftplib import FTP
import pandas as pd
import numpy as np

# Выгрузка TR_MAP и образование пролетов
def ftp_map():
    # Выгрузка TR_MAP из FTP сервера
    def dow_tr_map():
        ftp = FTP('8.8.8.8')  # IP ftp
        ftp.login('user', 'password')
        ftp.cwd('tr_map/results')
        files_list = ftp.nlst()
        df_list = pd.DataFrame(files_list, columns=["A"])
        tr_map = df_list.iloc[-1, :]['A']
        ftp.retrbinary("RETR " + tr_map, open(tr_map, 'wb').write)
        global df_map1
        df_map1 = pd.read_csv(tr_map)
        ftp.quit()


    # Построение пролетов из данных TR_MAP
    def mw_links():
        df_map2 = df_map1[["TN", "FE1", "FE2", "FE3", "FE4", "FE5", "FE6", "FE7", "FE8", "FE9", "FE10", "FE11", "FE12", "FE13"]]
        df_map2.FE1 = df_map2.FE1.replace(' ', np.nan, regex=True)
        df_map2 = df_map2.dropna(axis='index', how='any', subset=['TN'])
        df_map2 = df_map2.dropna(axis='index', how='any', subset=['FE1'])
        df_map2.replace(regex=[r"ML-"], value="", inplace=True)
        df_map2 = df_map2.dropna(axis='index', how='any', subset=['TN'])
        df_map3 = df_map2.assign(LINK1=df_map2.TN.astype(str) + ' - ' + df_map2.FE1.astype(str))
        df_map4 = df_map3.assign(LINK2=df_map3.FE1.astype(str) + ' - ' + df_map3.FE2.astype(str))
        df_map5 = df_map4.assign(LINK3=df_map4.FE2.astype(str) + ' - ' + df_map4.FE3.astype(str))
        df_map6 = df_map5.assign(LINK4=df_map5.FE3.astype(str) + ' - ' + df_map5.FE4.astype(str))
        df_map7 = df_map6.assign(LINK5=df_map6.FE4.astype(str) + ' - ' + df_map6.FE5.astype(str))
        df_map8 = df_map7.assign(LINK6=df_map7.FE5.astype(str) + ' - ' + df_map7.FE6.astype(str))
        df_map9 = df_map8.assign(LINK7=df_map8.FE6.astype(str) + ' - ' + df_map8.FE7.astype(str))
        df_map10 = df_map9.assign(LINK8=df_map9.FE7.astype(str) + ' - ' + df_map9.FE8.astype(str))
        df_map11 = df_map10.assign(LINK9=df_map10.FE8.astype(str) + ' - ' + df_map10.FE9.astype(str))
        df_map12 = df_map11.assign(LINK10=df_map11.FE9.astype(str) + ' - ' + df_map11.FE10.astype(str))
        df_map13 = df_map12.assign(LINK11=df_map12.FE10.astype(str) + ' - ' + df_map12.FE11.astype(str))
        df_map14 = df_map13.assign(LINK12=df_map13.FE11.astype(str) + ' - ' + df_map13.FE12.astype(str))
        df_map15 = df_map14.assign(LINK13=df_map14.FE12.astype(str) + ' - ' + df_map14.FE13.astype(str))
        df_vse = pd.concat([df_map15["LINK1"], df_map15["LINK2"], df_map15["LINK3"], df_map15["LINK4"], df_map15["LINK5"], df_map15["LINK6"], df_map15["LINK7"], df_map15["LINK8"], df_map15["LINK9"], df_map15["LINK10"], df_map15["LINK11"], df_map15["LINK12"], df_map15["LINK13"]])
        df_vse.columns = ['LINK']
        new_columns = ["LINK"]
        global df_vse1
        df_vse1 = pd.DataFrame(df_vse, columns = new_columns)


    # Нормализация данных
    def normal_data():
        df_vse2 = df_vse1[df_vse1.LINK != '  -  ']
        map2 = df_vse2.values.tolist()
        new_list = map(str, map2)
        arp = [i.replace(".1", "") for i in new_list]
        arp1 = [i.replace(".2", "") for i in arp]
        arp2 = [i.replace(".3", "") for i in arp1]
        all_link = pd.DataFrame(arp2)
        all_link.replace(regex=[r"MW."], value="", inplace=True)
        all_link.columns = ['NAME']
        all_link1 = all_link.drop_duplicates(subset=['NAME'], keep='first')
        all_link1['NAME'] = all_link1['NAME'].str[2:-2]
        all_link5 = all_link1.values.tolist()
        new_link = map(str, all_link5)
        new_link1 = [i.replace("[", "") for i in new_link]
        new_link2 = [i.replace("]", "") for i in new_link1]
        global new_link3
        new_link3 = [i.replace("'", "") for i in new_link2]


    # Удаление кривых пролетов
    def del_links():
        # Удаление пролетов где длинна символов меньше 11
        finish120 = []
        for word in new_link3:
            if len(word) < 11:
                break
            else:
                finish120.append(word)
        finish130 = pd.DataFrame(finish120)
        finish140 = finish130.values.tolist()
        finish150 = map(str, finish140)
        f6 = [i.replace("[", "") for i in finish150]
        f7 = [i.replace("]", "") for i in f6]
        f8 = [i.replace("'", "") for i in f7]
        # Удаление пролетов где название NE и FE одинаковые
        lll = []
        for i in f8:
            if i[:6] == i[-6:]:
                continue
            else:
                lll.append(i)
        global tr_map_links
        tr_map_links = pd.DataFrame(lll)
        tr_map_links.columns = ['NAME']
    del_links()
    return tr_map_links
