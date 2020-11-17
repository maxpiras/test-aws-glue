def read_wkr(start_date, end_date, tipo_calcolo, path_wkr):
    import pandas as pd
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

    #CASO CONSUNTIVO
    if tipo_calcolo == 'cons':
        print('consuntivo')
        #ESTRAZIONE E FILTRO WKR RISPETTO ALLA MAX DATA CON CONSUNTIVO
        max_data_consuntivo = df_wkr.loc[(df_wkr['TIPO'] == 'C') & (df_wkr['V_WKR'] == 'WKR_18')].groupby(['TIPO', 'V_WKR'])['GIORNO'].max().iloc[0]
        df_wkr = df_wkr.loc[(df_wkr['GIORNO'] <= max_data_consuntivo)&(df_wkr['TIPO'] == 'C') & (df_wkr['V_WKR'] == 'WKR_18')]
        #MERGE CON CALENDARIO ESPLOSO WKR-GIORNO PER TAPPARE I BUCHI
        df_calendar_wkr = df_calendar_wkr.merge(df_wkr, on = ['ZONA_CLIMATICA', 'GIORNO'], how = 'left')
        df_calendar_wkr['WKR'] = df_calendar_wkr['WKR'].where(~df_calendar_wkr['WKR'].isnull(),1)
        #df_calendar_wkr.to_csv('TEST_CONS.csv')
        return df_calendar_wkr
    elif tipo_calcolo == 'prev':
        print('prev')
    #CASO BEST
    elif tipo_calcolo == 'best':
        df_wkr['TOP_PRIORITY'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO'])['TIPO_PRIORITY'].transform('min')
        df_wkr['MAX_DATA_WKR'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO', 'TIPO_PRIORITY'])['DATA_WKR'].transform('max')
        df_wkr = df_wkr.loc[df_wkr['TIPO_PRIORITY'] == df_wkr['TOP_PRIORITY']]
        df_calendar_wkr = df_calendar_wkr.merge(df_wkr, on = ['ZONA_CLIMATICA', 'GIORNO'], how = 'left')
        df_calendar_wkr['WKR'] = df_calendar_wkr['WKR'].where(~df_calendar_wkr['WKR'].isnull(),1)
        df_calendar_wkr.to_csv('TEST_BEST.csv')
        return df_calendar_wkr
        print('best')
    else:
        print('error')
        
def main(start_date, end_date, tipo_calcolo, path_anagrafica_pdr, path_anagrafica_osservatori, path_wkr, filename):
    import pandas as pd
    import datetime as dt

    df_coef_res = pd.read_csv(path_to_data+'/profili_elaborati.csv')
    df_coef_res.columns = df_coef_res.columns.str.upper()
    df_coef_res['DATE'] = df_coef_res['DATE'].str.replace('-','')
    df_coef_res = df_coef_res.loc[(df_coef_res['DATE'] >= start_date) & (df_coef_res['DATE'] <= end_date)]

    df_wkr = read_wkr(start_date, end_date, tipo_calcolo, path_wkr)
    df_wkr.columns = df_wkr.columns.str.upper()
    df_wkr= df_wkr.rename(columns = {'GIORNO': 'DATE'})
    #print(df_coef_res)

    df_rcu = pd.read_csv(path_anagrafica_pdr)
    df_rcu.columns = df_rcu.columns.str.upper()
    df_rcu = df_rcu.rename(columns = {'STATION': 'STATION_FISICA'})
    #print(df_rcu)

    df_anagrafica_osservatori = pd.read_csv(path_anagrafica_osservatori)
    df_anagrafica_osservatori.columns = df_anagrafica_osservatori.columns.str.upper()
    df_anagrafica_osservatori['ZONA_CLIMATICA'] = df_anagrafica_osservatori['ZONA_CLIMATICA'].astype(str)
    #print(df_anagrafica_osservatori)
    import datetime
    datetime.datetime.now().strftime("%D%H:%M:%S")

    df_rcu['STATION_FISICA'] = df_rcu['STATION_FISICA'].astype(str)
    df_pp_pdr = df_coef_res.merge(df_rcu,on='PROFILO').merge(df_anagrafica_osservatori,on='STATION_FISICA',how='left')

    df_pp_pdr = df_pp_pdr.merge(df_wkr,on=['DATE','ZONA_CLIMATICA'],how='left')
    #df_pp_pdr = df_pp_pdr.where(~ df_pp_pdr['WKR'].isnull(),1)
    df_pp_pdr = df_pp_pdr.assign(K=df_pp_pdr['C_WKR']*df_pp_pdr['WKR']+df_pp_pdr['C_CONST'])
    df_pp_pdr = df_pp_pdr.assign(SMC=df_pp_pdr['K']*df_pp_pdr['CONSUMO_ANNUO']/100)

    df_pp_pdr.to_csv(filename)
    #print(df_pp_pdr)
