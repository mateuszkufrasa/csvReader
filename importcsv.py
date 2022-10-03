import pandas as pd
import numpy as np
import pyodbc
from datetime import datetime

rawData = pd.read_csv(r'C:\Users\mateu\Desktop\testimport.csv', encoding='ANSI', header=0, sep=';')
rawData.columns = rawData.columns.str.title()
rawData.columns = rawData.columns.str.replace('ą', 'a')
rawData.columns = rawData.columns.str.replace('ć', 'c')
rawData.columns = rawData.columns.str.replace('ę', 'e')
rawData.columns = rawData.columns.str.replace('ł', 'l')
rawData.columns = rawData.columns.str.replace('ń', 'n')
rawData.columns = rawData.columns.str.replace('ó', 'o')
rawData.columns = rawData.columns.str.replace('Ś', 'S')
rawData.columns = rawData.columns.str.replace('ś', 's')
rawData.columns = rawData.columns.str.replace('ź', 'z')
rawData.columns = rawData.columns.str.replace('ż', 'z')
rawData.columns = rawData.columns.str.replace(' ', '')
rawData['DataGodzina'] = pd.to_datetime(rawData['Doba']) + pd.to_timedelta(rawData['Godzina'] - 1, unit='h')
rawData.rename(columns={
    'NadwyzkaMocyDostepnaDlaOsp(7)+(9)-[(3)-(12)]-(13)': 'NadwyzkaMocyDostepnaDlaOsp',
    'PrzewidywanaGeneracjaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb(3)-(9)-(12)': 'PrzewidywanaGeneracjaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb',
    'PrognozowanaWielkoscNiedyspozycyjnosciWynikajacaZOgraniczenSieciowychWystepujacychWSieciPrzesylowejOrazSieciDystrybucyjnejWZakresieDostarczaniaEnergiiElektrycznej': 'PrognozowanaWielkoscNiedyspozycyjnosciWynikajacaZOgraniczenSieciowych'},
    inplace=True)
# remove non-breaking white space from the field using regular expression
rawData.MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRbDostepnaDlaOsp = rawData.MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRbDostepnaDlaOsp.astype(
    'string').replace('\xa0', '', regex=True).astype(np.int64)
# from sklearn.cluster import KMeans
#
# Kmean = KMeans(n_clusters=7)
# rawData.reset_index(drop=True)
# rawData.drop(['Doba', 'Godzina', 'DataGodzina'], axis=1, inplace=True)
# Kmean.fit(rawData)
# print(Kmean.cluster_centers_)
# sample_test=np.array([13914,1504,2793,12275,10242,7449,8065,5682,0,-1600,0,12891,0])
# second_test=sample_test.reshape(1, -1)
# print(Kmean.predict(second_test))
conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=DESKTOP-GU0D16O\MYSQLSERVER1;'
                      'Database=Energia;'
                      'Trusted_Connection=yes;')

cursor = conn.cursor()
now = datetime.now()
res = None
for index, row in rawData.iterrows():
    cursor.execute("select max([NrTransakcji]) FROM [Energia].[dbo].[Table_1]")
    res = cursor.fetchval()
    if res == None:
        res = 0
    else:
        res +=1


conn.commit()
for index, row in rawData.iterrows():
    cursor.execute("INSERT INTO Energia.dbo.Table_1 "
                   "(DataPL"
                   ",NrTransakcji"
                   ",Godzina"
                   ",PrognozowaneZapotrzebowanieSieci"
                   ",WymaganaRezerwaMocyOsp"
                   ",NadwyzkaMocyDostepnaDlaOsp"
                   ",MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb"
                   ",MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRbDostepnaDlaOsp"
                   ",PrzewidywanaGeneracjaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb"
                   ",PrognozowanaGeneracjaJwIMagazynowEnergiiNieSwiadczacychUslugBilansujacychWRamachRb"
                   ",PrognozowanaSumarycznaGeneracjaŹrodelWiatrowych"
                   ",PrognozowanaSumarycznaGeneracjaŹrodelFotowoltaicznych"
                   ",PlanowaneSaldoWymianyMiedzysystemowej"
                   ",PrognozowanaWielkoscNiedyspozycyjnosciWynikajacaZOgraniczenSieciowych"
                   ",PrzewidywanaGeneracjaZasobowWytworczychNieobjetychObowiazkamiMocowymi"
                   ",ObowiazkiMocoweWszystkichJednostekRynkuMocy"
                   ",DataGodzina"
                   ",CzasZapisu) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   row.Doba
                   , res
                   , row.Godzina
                   , row.PrognozowaneZapotrzebowanieSieci
                   , row.WymaganaRezerwaMocyOsp
                   , row.NadwyzkaMocyDostepnaDlaOsp
                   , row.MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb
                   , row.MocDyspozycyjnaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRbDostepnaDlaOsp
                   , row.PrzewidywanaGeneracjaJwIMagazynowEnergiiSwiadczacychUslugiBilansujaceWRamachRb
                   , row.PrognozowanaGeneracjaJwIMagazynowEnergiiNieSwiadczacychUslugBilansujacychWRamachRb
                   , row.PrognozowanaSumarycznaGeneracjaŹrodelWiatrowych
                   , row.PrognozowanaSumarycznaGeneracjaŹrodelFotowoltaicznych
                   , row.PlanowaneSaldoWymianyMiedzysystemowej
                   , row.PrognozowanaWielkoscNiedyspozycyjnosciWynikajacaZOgraniczenSieciowych
                   , row.PrzewidywanaGeneracjaZasobowWytworczychNieobjetychObowiazkamiMocowymi
                   , row.ObowiazkiMocoweWszystkichJednostekRynkuMocy
                   , row.DataGodzina
                   , now
                   )
conn.commit()
cursor.close()
