import pandas as pd
from datetime import datetime as dt
from datetime import timedelta, datetime
import calendar as cl
import numpy as np


#ALGORITMO 1 FUNZIONI

def read_profili(path_to_data):
    df_profili = pd.read_csv(path_to_data +'preprocessato/sistema/coefficienti/external/2020/profili_elaborati.csv')
    df_profili.columns = df_profili.columns.str.upper()
    df_profili['TIPOLOGIA'] = df_profili['PROFILO'].str.slice(start=0, stop=1)
    df_profili['DATE'] = df_profili['DATE'].str.replace('-','')
    df_profili['DATE']=pd.to_datetime(df_profili['DATE'],format='%Y%m%d')
    df_profili = df_profili.loc[(df_profili['DATE'] >= start_date) & (df_profili['DATE'] <= end_date)]
    df_profili['ANNO_MESE'] = df_profili['DATE'].astype(str).str.slice(start=0, stop=7)
    df_profili = df_profili[['PROFILO', 'DATE', 'ANNO_MESE', 'C_WKR', 'C_CONST', 'TIPOLOGIA']]
    return df_profili

def read_wkr(start_date, end_date, tipo_calcolo, path_wkr):
    #LETTURA FILE WKR
    df_wkr = pd.read_csv(path_wkr)
    df_wkr.columns= df_wkr.columns.str.upper()
    df_wkr['GIORNO'] = df_wkr['GIORNO'].apply(lambda x: str(x))
    df_wkr['ZONA_CLIMATICA'] = df_wkr['ZONA_CLIMATICA'].astype(str).str[-2:]

    #FILTRO WKR IN BASE ALLE DATE SELEZIONATE (START-END)
    df_wkr = df_wkr.loc[(df_wkr['GIORNO'] >= start_date) & (df_wkr['GIORNO'] <= end_date)]
    df_wkr.to_csv('TEST_WKR.csv')

    #DEFINIZIONE VALORE PRIORITA' PER TIPO E ORARIO DI ARRIVO (V_WKR)
    di_tipo = {"C":1, "I":2, "P":3, "P1":4, "P2":5, "P3":6, "P4":7,"P5":8 }
    di_orario = {"WKR_18": 1, "WKR_11": 2}
    df_wkr['TIPO_PRIORITY'] = df_wkr['TIPO'].map(di_tipo) * 10 + df_wkr['V_WKR'].map(di_orario)

    #DEFINIZIONE CALENDARIO ESPLOSO WKR-GIORNO
    df_wkr_new = df_wkr[['ZONA_CLIMATICA']].drop_duplicates()
    df_wkr_new['KEY'] = 0

    df_calendar = pd.DataFrame({"GIORNO": pd.date_range(start_date, end_date)})
    df_calendar['GIORNO'] = df_calendar['GIORNO'].dt.strftime('%Y-%m-%d').str.replace('-','')
    df_calendar['KEY'] = 0
    df_calendar['TIPO_1'] = '1'
    df_calendar_wkr = df_calendar.merge(df_wkr_new, on ='KEY')[['ZONA_CLIMATICA', 'GIORNO']]
    #df_calendar_wkr.to_csv('TEST_CALENDAR.csv')
    
    df_wkr['TOP_PRIORITY'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO'])['TIPO_PRIORITY'].transform('min')
    df_wkr['MAX_DATA_WKR'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO', 'TIPO_PRIORITY'])['DATA_WKR'].transform('max')
    df_wkr = df_wkr.loc[df_wkr['DATA_WKR'] == df_wkr['MAX_DATA_WKR']]

    #CASO CONSUNTIVO
    if tipo_calcolo == 'cons':
        print('consuntivo')
        #ESTRAZIONE E FILTRO WKR RISPETTO ALLA MAX DATA CON CONSUNTIVO
        max_data_consuntivo = df_wkr.loc[(df_wkr['TIPO'] == 'C') & (df_wkr['V_WKR'] == 'WKR_18')].groupby(['TIPO', 'V_WKR'])['GIORNO'].max().iloc[0]
        df_wkr = df_wkr.loc[(df_wkr['GIORNO'] <= max_data_consuntivo)&(df_wkr['TIPO'] == 'C') & (df_wkr['V_WKR'] == 'WKR_18')]
    elif tipo_calcolo == 'prev':
        print('prev')       
        #ESTRAZIONE E FILTRO WKR RISPETTO ALLA MAX DATA CON I
        max_data_prev = df_wkr.loc[(df_wkr['TIPO'] == 'I') & (df_wkr['V_WKR'] == 'WKR_11')].groupby(['TIPO', 'V_WKR'])['GIORNO'].max().iloc[0]
        df_wkr = df_wkr.loc[(df_wkr['GIORNO'] <= max_data_prev)&(df_wkr['TIPO'] == 'I') & (df_wkr['V_WKR'] == 'WKR_11')]
    #CASO BEST
    elif tipo_calcolo == 'best':
        print('best')
        df_wkr = df_wkr.loc[(df_wkr['TIPO_PRIORITY'] == df_wkr['TOP_PRIORITY']) & (df_wkr['DATA_WKR'] == df_wkr['MAX_DATA_WKR'])]
    #CASO WKR SELEZIONATO 
    elif (len(tipo_calcolo)==4) | (len(tipo_calcolo) == 5):
        v_wkr = tipo_calcolo[-2:].upper()
        tipo_wkr = tipo_calcolo[:(len(tipo_calcolo)-3)].upper()
        print('wkr selezionato ' + tipo_wkr + v_wkr)
        #ESTRAZIONE E FILTRO WKR RISPETTO ALLA MAX DATA CON CONSUNTIVO
        max_data_consuntivo = df_wkr.loc[(df_wkr['TIPO'] == tipo_wkr) & (df_wkr['V_WKR'] == ('WKR_' + v_wkr))].groupby(['TIPO', 'V_WKR'])['GIORNO'].max().iloc[0]
        df_wkr = df_wkr.loc[(df_wkr['GIORNO'] <= max_data_consuntivo)&(df_wkr['TIPO'] ==  tipo_wkr) & (df_wkr['V_WKR'] ==  ('WKR_' + v_wkr))]
    else:
        print('error')
        
    #MERGE CON CALENDARIO ESPLOSO WKR-GIORNO PER TAPPARE I BUCHI
    df_calendar_wkr = df_calendar_wkr.merge(df_wkr, on = ['ZONA_CLIMATICA', 'GIORNO'], how = 'left')
    df_calendar_wkr['WKR'] = df_calendar_wkr['WKR'].where(~df_calendar_wkr['WKR'].isnull(),1)    
    df_calendar_wkr= df_calendar_wkr.rename(columns = {'GIORNO': 'DATE'})
    df_calendar_wkr['DATE']=pd.to_datetime(df_calendar_wkr['DATE'],format='%Y%m%d')
    df_calendar_wkr = df_calendar_wkr[['ZONA_CLIMATICA', 'DATE', 'WKR']]
    return df_calendar_wkr

def read_pdr(path_anagrafica_pdr):
    df_pdr = pd.read_csv(path_anagrafica_pdr)
    df_pdr.columns = df_pdr.columns.str.upper()
    df_pdr['DATA_INIZIO']=pd.to_datetime(df_pdr['DATA_INIZIO'],format='%Y%m%d')
    df_pdr['DATA_FINE']=pd.to_datetime(df_pdr['DATA_FINE'],format='%Y%m%d')
    df_pdr = df_pdr[['PDR', 'STATION', 'PIVA', 'TRATTAMENTO', 'PROFILO', 'CONSUMO_ANNUO','DATA_INIZIO','DATA_FINE']]

    df_pdr['PDR'] = df_pdr['PDR'].where(~df_pdr['PDR'].isnull(),0).astype(int).astype(str).apply(lambda x: x.zfill(14))
    df_pdr['PIVA'] = df_pdr['PIVA'].where(~df_pdr['PIVA'].isnull(),0).astype(int).astype(str).apply(lambda x: x.zfill(11))
    df_pdr['STATION'] = df_pdr['STATION'].astype(str)
    di_piva = {"08526440154":'edison_energia', "03678410758": 'societa_gruppo', "05044850823": 'societa_gruppo'}
    df_pdr['SOCIETA'] = df_pdr['PIVA'].map(di_piva)
    df_pdr['SOCIETA'] = df_pdr['SOCIETA'].where(~df_pdr['SOCIETA'].isnull(), 'grossisti')
    df_pdr['TRATTAMENTO'] = df_pdr['TRATTAMENTO'].where(~df_pdr['TRATTAMENTO'].isnull(), 'ND')
    di_trattamento = {'Y': 'Y', 'M': 'GM', 'G': 'GM', 'ND' : 'ND'}
    df_pdr['TRATTAMENTO_AGG'] = df_pdr['TRATTAMENTO'].map(di_trattamento)
    df_pdr['STATION']=df_pdr['STATION'].astype(str)
    return df_pdr

def read_osservatori(path_anagrafica_osservatori):
    df_anagrafica_osservatori = pd.read_csv(path_anagrafica_osservatori)
    df_anagrafica_osservatori.columns = df_anagrafica_osservatori.columns.str.upper()
    df_anagrafica_osservatori = df_anagrafica_osservatori.loc[df_anagrafica_osservatori['STATION'] == df_anagrafica_osservatori['STATION_FISICA']]
    df_anagrafica_osservatori = df_anagrafica_osservatori[['STATION', 'ZONA_CLIMATICA']]
    df_anagrafica_osservatori['ZONA_CLIMATICA'] = df_anagrafica_osservatori['ZONA_CLIMATICA'].astype(str)
    df_anagrafica_osservatori['STATION']=df_anagrafica_osservatori['STATION'].astype(str)
    return df_anagrafica_osservatori

def comp(dateS, dateE, NUM_M):
    """Metodo per dividere l'intervallo di competenza in base al numero di mesi.

    Input:  dateS       --> colonna data inizio competenza
            dateE       --> colonna data fine competenza
            NUM_M       --> colonna intervallo mensile di competenza

    Output: StartDate   --> colonna data inizio competenza mensile
            EndDate     --> colonna data fine competenza mensile
            """
    StartDate= []
    EndDate = []
    for i in range(NUM_M):
        if i == 0:
            if (dateS.month == dateE.month and dateS.year == dateE.year):
                StartDate.append(dateS)
                EndDate.append(dateE)
            else:
                StartDate.append(dateS)
                dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
                EndDate.append(dateS)
                dateS = dateS + timedelta(1)
        elif i == NUM_M - 1:
            StartDate.append(dateS)
            EndDate.append(dateE)
        else:
            StartDate.append(dateS)
            dateS = dateS.replace(day=cl.monthrange(dateS.year, dateS.month)[1])
            EndDate.append(dateS)
            dateS = dateS + timedelta(1)
    return StartDate, EndDate

def mergeDati(df_profili, df_pdr, df_anagrafica_osservatori, df_wkr, anno_mese, societa, path_output):
    print('computation for ' + anno_mese + ' started')
    df_pp_pdr = df_pdr.merge(df_anagrafica_osservatori,on='STATION',how='left')
    print('merge pdr zona climatica')
    df_pp_pdr = df_profili.merge(df_pp_pdr,on=['PROFILO'])
    print('merge pdr profili done')
    df_pp_pdr = df_pp_pdr.loc[(df_pp_pdr['DATE'] >= df_pp_pdr['DATA_INIZIO']) & (df_pp_pdr['DATE'] <= df_pp_pdr['DATA_FINE'])]
    print('filter pdr by date')
    df_pp_pdr = df_pp_pdr.merge(df_wkr,on=['DATE','ZONA_CLIMATICA'],how='left')
    print('merge pdr wkr')
    df_pp_pdr = df_pp_pdr.assign(K=df_pp_pdr['C_WKR']*df_pp_pdr['WKR']+df_pp_pdr['C_CONST'])
    df_pp_pdr = df_pp_pdr.assign(K_NO_WKR=df_pp_pdr['C_WKR']*1+df_pp_pdr['C_CONST'])
    df_pp_pdr = df_pp_pdr.assign(SMC=df_pp_pdr['K']*df_pp_pdr['CONSUMO_ANNUO']/100)
    df_pp_pdr = df_pp_pdr.assign(SMC_NO_WKR=df_pp_pdr['K_NO_WKR']*df_pp_pdr['CONSUMO_ANNUO']/100)
    print('compute k+smc')
    df_pp_pdr_aggr_societa_tipo_tratt = df_pp_pdr.groupby(['SOCIETA', 'TRATTAMENTO_AGG', 'TIPOLOGIA', 'DATE', 'ANNO_MESE', 'WKR']).agg(SMC=pd.NamedAgg(column='SMC', aggfunc='sum'), SMC_NO_WKR=pd.NamedAgg(column='SMC_NO_WKR', aggfunc='sum'), K=pd.NamedAgg(column='K', aggfunc='sum'), K_NO_WKR=pd.NamedAgg(column='K_NO_WKR', aggfunc='sum'), CONSUMO_ANNUO=pd.NamedAgg(column='CONSUMO_ANNUO', aggfunc='sum'), C_CONST=pd.NamedAgg(column='C_CONST', aggfunc='sum'), C_WKR=pd.NamedAgg(column='C_WKR', aggfunc='sum')).reset_index()
    print('computed aggregato grafico')
    df_pp_pdr_aggr_station_tipo_tratt = df_pp_pdr.groupby(['TRATTAMENTO', 'TIPOLOGIA', 'STATION', 'DATE', 'ANNO_MESE', 'WKR']).agg(SMC=pd.NamedAgg(column='SMC', aggfunc='sum'), SMC_NO_WKR=pd.NamedAgg(column='SMC_NO_WKR', aggfunc='sum'), K=pd.NamedAgg(column='K', aggfunc='sum'), K_NO_WKR=pd.NamedAgg(column='K_NO_WKR', aggfunc='sum'), CONSUMO_ANNUO=pd.NamedAgg(column='CONSUMO_ANNUO', aggfunc='sum'), C_CONST=pd.NamedAgg(column='C_CONST', aggfunc='sum'), C_WKR=pd.NamedAgg(column='C_WKR', aggfunc='sum')).reset_index()
    print('computed aggregato station tipologia trattamento')
    df_pp_pdr_aggr_station_societa_profilo_tratt = df_pp_pdr.groupby(['TRATTAMENTO', 'PROFILO', 'SOCIETA', 'PIVA', 'STATION', 'DATE', 'ANNO_MESE', 'WKR']).agg(SMC=pd.NamedAgg(column='SMC', aggfunc='sum'), SMC_NO_WKR=pd.NamedAgg(column='SMC_NO_WKR', aggfunc='sum'), K=pd.NamedAgg(column='K', aggfunc='sum'), K_NO_WKR=pd.NamedAgg(column='K_NO_WKR', aggfunc='sum'), CONSUMO_ANNUO=pd.NamedAgg(column='CONSUMO_ANNUO', aggfunc='sum'), C_CONST=pd.NamedAgg(column='C_CONST', aggfunc='sum'), C_WKR=pd.NamedAgg(column='C_WKR', aggfunc='sum')).reset_index()
    df_pp_pdr_aggr_non_progr = df_pp_pdr.groupby('PDR')
    print('computed aggregato station societa profilo trattamento')
    
    df_pp_pdr_dett = df_pp_pdr[['SOCIETA', 'PIVA', 'TRATTAMENTO', 'TIPOLOGIA', 'PROFILO', 'ZONA_CLIMATICA', 'STATION', 'PDR', 'DATE', 'WKR', 'SMC', 'CONSUMO_ANNUO']]
    print('extract subset of fields for dettaglio')
    df_pp_pdr_dett.loc[df_pp_pdr_dett['TRATTAMENTO'] == 'Y'].to_csv(path_output + anno_mese + '/' + 'dettaglio/dettaglio_' + societa + '_y.csv')
    print('dettaglio y written: ' + df_pp_pdr_dett.loc[df_pp_pdr_dett['TRATTAMENTO'] == 'Y']['PDR'].count().astype(str))
    df_pp_pdr_dett.loc[df_pp_pdr_dett['TRATTAMENTO'] != 'Y'].to_csv(path_output + anno_mese + '/' + 'dettaglio/dettaglio_' + societa + '_gm.csv')
    print('dettaglio gm written: ' + df_pp_pdr_dett.loc[(df_pp_pdr_dett['TRATTAMENTO'] != 'Y')]['PDR'].count().astype(str))
    df_pp_pdr_checks = df_pp_pdr_dett.loc[(df_pp_pdr_dett['SMC'].isnull()) | (df_pp_pdr_dett['WKR'].isnull())]
    print('checks computed: wkr null ' +  df_pp_pdr_dett.loc[df_pp_pdr_dett['SMC'].isnull()]['PDR'].count().astype(str) + ' smc null ' + df_pp_pdr_dett.loc[df_pp_pdr_dett['WKR'].isnull()]['PDR'].count().astype(str))
    
    return df_pp_pdr_aggr_societa_tipo_tratt, df_pp_pdr_aggr_station_tipo_tratt, df_pp_pdr_aggr_station_societa_profilo_tratt, df_pp_pdr_checks
	
def main(start_date, end_date, tipo_calcolo, path_anagrafica_pdr, path_anagrafica_pdr2, path_anagrafica_osservatori, path_wkr, path_output):


    #BASE PATH SU S3
    path_to_data = 's3://zus-qa-s3/'
    
    #DIVISIONE PERIODO DI CONTO IN SOTTO PERIODI CON LUNGHEZZA MASSIMA DI UN MESE, START_COUNT E END_COUNT SONO I NOMI DELLE DATE CHE DELIMITANO GLI INTERVALLI
    date_format='%Y%m%d'
    start_d=datetime.strptime(start_date,date_format)
    end_d=datetime.strptime(end_date,date_format)
    N_NUM=end_d.month + (12 - start_d.month + 1) + (end_d.year - start_d.year - 1) * 12
    START_COUNT,END_COUNT=comp(start_d,end_d,N_NUM)
    year = start_date[:4]
    
    #LETTURA PROFILI ELABORATI
    df_profili = read_profili(path_to_data)
    print('read from ' + path_to_data +'preprocessato/sistema/coefficienti/external/' + year + '/profili_elaborati.csv')

    #LETTURA WKR
    df_wkr = read_wkr(start_date, end_date, tipo_calcolo, path_to_data + path_wkr)
    print('read from ' + path_to_data + path_wkr)

    #LETTURA ANAGRAFICA PDR 1
    df_pdr= read_pdr(path_to_data + path_anagrafica_pdr)
    print('read from ' + path_to_data + path_anagrafica_pdr)
    
    #LETTURA ANAGRAFICA PDR 2
    if path_anagrafica_pdr2 and (tipo_calcolo == 'cons'):
        df_pdr2 = read_pdr(path_to_data+path_anagrafica_pdr2)
        print('read from ' + path_to_data+path_anagrafica_pdr2)

    #LETTURA ANAGRAFICA OSSERVATORI
    df_anagrafica_osservatori = read_osservatori(path_to_data + path_anagrafica_osservatori)
    print('read from ' + path_to_data + path_anagrafica_osservatori)

    for i, j in zip(START_COUNT, END_COUNT):
        print(i, j)
        df_coef_month = df_profili.loc[(df_profili['DATE'] >= i) & (df_profili['DATE'] <= j)]
        anno_mese = df_coef_month['ANNO_MESE'].unique()[0].replace("-", "").replace("/","")
        print('coef filtered per month')
        df_pdr_month = df_pdr.loc[(df_pdr['DATA_FINE'] >= i) & (df_pdr['DATA_INIZIO'] <= j)]
        if df_pdr_month.empty:
            df_pdr_month = df_pdr2.loc[(df_pdr2['DATA_FINE'] >= i) & (df_pdr2['DATA_INIZIO'] <= j)]
            print('using anagrafica pdr2 for ', k, i, j)
        df_pdr_month_ee = df_pdr_month.loc[df_pdr_month['SOCIETA'] == 'edison_energia']
        print('subsetting edison energia')
        df_pdr_month_sg = df_pdr_month.loc[df_pdr_month['SOCIETA'] == 'societa_gruppo']
        print('subsetting societa gruppo')
        df_pdr_month_gr = df_pdr_month.loc[df_pdr_month['SOCIETA'] == 'grossisti']
        print('subsetting grossisti')
        
        df_pp_pdr_aggr_month_ee, df_pp_pdr_aggr_station_tipo_tratt_month_ee, df_pp_pdr_aggr_station_societa_profilo_tratt_month_ee, df_pp_pdr_checks_ee = mergeDati(df_coef_month, df_pdr_month_ee, df_anagrafica_osservatori, df_wkr, anno_mese, 'ee', path_to_data + path_output)
        print('computed edison energia')
        df_pp_pdr_aggr_month_sg, df_pp_pdr_aggr_station_tipo_tratt_month_sg, df_pp_pdr_aggr_station_societa_profilo_tratt_month_sg, df_pp_pdr_checks_sg = mergeDati(df_coef_month, df_pdr_month_sg, df_anagrafica_osservatori, df_wkr, anno_mese, 'sg', path_to_data + path_output)
        print('computed societa gruppo')
        df_pp_pdr_aggr_month_gr, df_pp_pdr_aggr_station_tipo_tratt_month_gr, df_pp_pdr_aggr_station_societa_profilo_tratt_month_gr, df_pp_pdr_checks_gr = mergeDati(df_coef_month, df_pdr_month_gr, df_anagrafica_osservatori, df_wkr, anno_mese, 'gr', path_to_data + path_output)
        print('computed grossisti')
        df_pp_pdr_aggr  = df_pp_pdr_aggr_month_ee.append(df_pp_pdr_aggr_month_sg).append(df_pp_pdr_aggr_month_gr)
        df_pp_pdr_aggr_station_tipo_tratt = df_pp_pdr_aggr_station_tipo_tratt_month_ee.append(df_pp_pdr_aggr_station_tipo_tratt_month_sg).append(df_pp_pdr_aggr_station_tipo_tratt_month_gr)
        df_pp_pdr_aggr_station_societa_profilo_tratt = df_pp_pdr_aggr_station_societa_profilo_tratt_month_ee.append(df_pp_pdr_aggr_station_societa_profilo_tratt_month_sg).append(df_pp_pdr_aggr_station_societa_profilo_tratt_month_gr)
        df_pp_pdr_checks = df_pp_pdr_checks_ee.append(df_pp_pdr_checks_sg).append(df_pp_pdr_checks_gr)
        print('computation ended')
        df_pp_pdr_aggr.to_csv(path_to_data + path_output + anno_mese + "/" + 'aggregato_societa_tipo_tratt.csv')
        print('aggregato_grafico written')
        df_pp_pdr_aggr_station_tipo_tratt.to_csv(path_to_data + path_output  + anno_mese + "/" +  'aggregato_station_tipo_tratt.csv')
        print('aggregato_station_tipo_tratt written')
        df_pp_pdr_aggr_station_societa_profilo_tratt.to_csv(path_to_data + path_output  + anno_mese + "/" +  'aggregato_station_societa_profilo_tratt.csv')
        print('aggregato_station_societa_profilo_tratt written')
        df_pp_pdr_checks.to_csv(path_to_data + path_output  + anno_mese + "/" +  'anomalie_dettaglio.csv')
        df_pp_pdr_checks['TOT_CONSUMO_ANNUO'] = df_pp_pdr_checks.groupby(['PDR']).agg(TOT_CONSUMO_ANNUO=pd.NamedAgg(column='CONSUMO_ANNUO', aggfunc='mean')).reset_index()['TOT_CONSUMO_ANNUO'].sum()
        df_pp_pdr_checks['TOT_PDR'] = len(df_pp_pdr_checks['PDR'].unique())
        df_pp_pdr_kpi_checks = df_pp_pdr_checks[['TOT_PDR', 'TOT_CONSUMO_ANNUO']].drop_duplicates()
        df_pp_pdr_kpi_checks.to_csv(path_to_data + path_output  + anno_mese + "/" +  'anomalie_aggregato.csv')
        print('dettaglio anomalie written')
    print('all months have been computed')
    return (path_to_data + path_output)
