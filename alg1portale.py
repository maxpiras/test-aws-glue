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
    
    df_wkr['TOP_PRIORITY'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO'])['TIPO_PRIORITY'].transform('min')
    df_wkr['MAX_DATA_WKR'] = df_wkr.groupby(['ZONA_CLIMATICA', 'GIORNO', 'TIPO_PRIORITY'])['DATA_WKR'].transform('max')
    df_wkr = df_wkr.loc[df_wkr['DATA_WKR'] == df_wkr['MAX_DATA_WKR']]

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
        #ESTRAZIONE E FILTRO WKR RISPETTO ALLA MAX DATA CON I
        max_data_prev = df_wkr.loc[(df_wkr['TIPO'] == 'I') & (df_wkr['V_WKR'] == 'WKR_11')].groupby(['TIPO', 'V_WKR'])['GIORNO'].max().iloc[0]
        df_wkr = df_wkr.loc[(df_wkr['GIORNO'] <= max_data_prev)&(df_wkr['TIPO'] == 'I') & (df_wkr['V_WKR'] == 'WKR_11')]
        
        #MERGE CON CALENDARIO ESPLOSO WKR-GIORNO PER TAPPARE I BUCHI
        df_calendar_wkr = df_calendar_wkr.merge(df_wkr, on = ['ZONA_CLIMATICA', 'GIORNO'], how = 'left')
        df_calendar_wkr['WKR'] = df_calendar_wkr['WKR'].where(~df_calendar_wkr['WKR'].isnull(),1)
        return df_calendar_wkr
        #df_calendar_wkr.to_csv('TEST_CONS.csv')
    #CASO BEST
    elif tipo_calcolo == 'best':
        print('best')
        df_wkr = df_wkr.loc[(df_wkr['TIPO_PRIORITY'] == df_wkr['TOP_PRIORITY']) & (df_wkr['DATA_WKR'] == df_wkr['MAX_DATA_WKR'])]
        df_calendar_wkr = df_calendar_wkr.merge(df_wkr, on = ['ZONA_CLIMATICA', 'GIORNO'], how = 'left')
        df_calendar_wkr['WKR'] = df_calendar_wkr['WKR'].where(~df_calendar_wkr['WKR'].isnull(),1)
        df_calendar_wkr.to_csv('TEST_BEST.csv')
        return df_calendar_wkr
    else:
        print('error')
        
def write_to_csv(path_to_data, path_output, df_pp_pdr):
    print('writing edison energia y in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'edison_energia') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'edison_energia') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].head(250000).to_csv(path_to_data + path_output + 'edison_energia_y.csv')
    
    print('writing edison energia gm in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'edison_energia') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'GM')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'edison_energia') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'GM')].head(250000).to_csv(path_to_data + path_output + 'edison_energia_gm.csv')
    
    print('writing societa gruppo y in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'societa_gruppo') & (df_pp_pdr['TRATTAMENTO_AGG']=='GM')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'societa_gruppo') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].head(250000).to_csv(path_to_data + path_output + 'societa_gruppo_y.csv')
    
    print('writing societa gruppo gm in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'societa_gruppo') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'grossisti') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'GM')].head(250000).to_csv(path_to_data + path_output + 'societa_gruppo_gm.csv')
    
    print('writing grossisti y in ' + path_to_data + path_output  + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'grossisti') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'GM')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'grossisti') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].head(250000).to_csv(path_to_data + path_output + 'grossisti_y.csv')
    
    print('writing grossisti gm in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].loc[(df_pp_pdr['SOCIETA'] == 'grossisti') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'Y')].count().astype(str))
    df_pp_pdr.loc[(df_pp_pdr['SOCIETA'] == 'grossisti') & (df_pp_pdr['TRATTAMENTO_AGG'] == 'GM')].head(250000).to_csv(path_to_data + path_output + 'grossisti_gm.csv')
    
    print('writing edison energia complessivo in ' + path_to_data + path_output + ' COUNT: ' + df_pp_pdr['PDR'].count().astype(str))
    df_pp_pdr.to_csv(path_to_data + path_output + 'complessivo.csv')

    return df_pp_pdr['PDR'].count()
        
def main(start_date, end_date, tipo_calcolo, path_anagrafica_pdr, path_anagrafica_osservatori, path_wkr, path_output):
    import pandas as pd
    import datetime as dt

    path_to_data = 's3://zus-qa-s3/'
    df_coef_res = pd.read_csv(path_to_data +'elaborato/sistema/coefficienti/external/2020/profili_elaborati.csv')
    print('reading from ' + path_to_data +'elaborato/sistema/coefficienti/external/2020/profili_elaborati.csv')
    df_coef_res.columns = df_coef_res.columns.str.upper()
    df_coef_res['TIPOLOGIA'] = df_coef_res['PROFILO'].str.slice(start=0, stop=1)
    df_coef_res['DATE'] = df_coef_res['DATE'].str.replace('-','')
    df_coef_res = df_coef_res[['PROFILO', 'DATE', 'C_WKR', 'C_CONST', 'TIPOLOGIA']]
    df_coef_res = df_coef_res.loc[(df_coef_res['DATE'] >= start_date) & (df_coef_res['DATE'] <= end_date)]

    df_wkr = read_wkr(start_date, end_date, tipo_calcolo, path_to_data + path_wkr)
    print('reading from ' + path_to_data + path_wkr)
    df_wkr.columns = df_wkr.columns.str.upper()
    df_wkr= df_wkr.rename(columns = {'GIORNO': 'DATE'})
    df_wkr = df_wkr[['ZONA_CLIMATICA', 'DATE', 'WKR']]
    #print(df_coef_res)

    df_rcu = pd.read_csv(path_to_data + path_anagrafica_pdr)
    print('reading from ' + path_to_data + path_anagrafica_pdr)
    df_rcu.columns = df_rcu.columns.str.upper()
    df_rcu = df_rcu[['PDR', 'STATION', 'PIVA', 'TRATTAMENTO', 'PROFILO', 'CONSUMO_ANNUO']]
    df_rcu['PIVA'] = df_rcu['PIVA'].astype(str).apply(lambda x: x.zfill(11))
    #print(df_rcu)

    df_anagrafica_osservatori = pd.read_csv(path_to_data + path_anagrafica_osservatori)
    print('reading from ' + path_to_data + path_anagrafica_osservatori)
    df_anagrafica_osservatori.columns = df_anagrafica_osservatori.columns.str.upper()
    df_anagrafica_osservatori = df_anagrafica_osservatori[['STATION', 'STATION_FISICA', 'ZONA_CLIMATICA']]
    df_anagrafica_osservatori = df_anagrafica_osservatori.loc[df_anagrafica_osservatori['STATION'] == df_anagrafica_osservatori['STATION_FISICA']]
    df_anagrafica_osservatori['ZONA_CLIMATICA'] = df_anagrafica_osservatori['ZONA_CLIMATICA'].astype(str)
    #print(df_anagrafica_osservatori)
    import datetime
    datetime.datetime.now().strftime("%D%H:%M:%S")

    df_rcu['STATION'] = df_rcu['STATION'].astype(str)
    df_pp_pdr = df_coef_res.merge(df_rcu,on='PROFILO').merge(df_anagrafica_osservatori,on='STATION',how='left')

    df_pp_pdr = df_pp_pdr.merge(df_wkr,on=['DATE','ZONA_CLIMATICA'],how='left')
    #df_pp_pdr = df_pp_pdr.where(~ df_pp_pdr['WKR'].isnull(),1)
    df_pp_pdr = df_pp_pdr.assign(K=df_pp_pdr['C_WKR']*df_pp_pdr['WKR']+df_pp_pdr['C_CONST'])
    df_pp_pdr = df_pp_pdr.assign(K_NO_WKR=df_pp_pdr['C_WKR']*1+df_pp_pdr['C_CONST'])
    df_pp_pdr = df_pp_pdr.assign(SMC=df_pp_pdr['K']*df_pp_pdr['CONSUMO_ANNUO']/100)
    df_pp_pdr = df_pp_pdr.assign(SMC_NO_WKR=df_pp_pdr['K_NO_WKR']*df_pp_pdr['CONSUMO_ANNUO']/100)

    df_pp_pdr.to_csv(path_to_data + path_output)
    di_piva = {"08526440154":'edison_energia', "03678410758": 'societa_gruppo', "05044850823": 'societa_gruppo'}
    df_pp_pdr['SOCIETA'] = df_pp_pdr['PIVA'].map(di_piva)
    df_pp_pdr['SOCIETA'] = df_pp_pdr['SOCIETA'].where(~df_pp_pdr['SOCIETA'].isnull(), 'grossisti')
    di_trattamento = {'Y': 'Y', 'M': 'GM', 'G': 'GM'}
    df_pp_pdr['TRATTAMENTO_AGG'] = df_pp_pdr['TRATTAMENTO'].map(di_trattamento)
    df_pp_pdr['TRATTAMENTO_AGG'] = df_pp_pdr['TRATTAMENTO_AGG'].where(~df_pp_pdr['TRATTAMENTO_AGG'].isnull(), '?')
    
    #write_to_csv(path_to_data, path_output, df_pp_pdr)
    
    df_pp_pdr_aggr = df_pp_pdr.groupby(['SOCIETA', 'TRATTAMENTO_AGG', 'TIPOLOGIA', 'DATE']).agg(SMC=pd.NamedAgg(column='SMC', aggfunc='sum'), SMC_NO_WKR=pd.NamedAgg(column='SMC_NO_WKR', aggfunc='sum'), K=pd.NamedAgg(column='K', aggfunc='sum'), K_NO_WKR=pd.NamedAgg(column='K_NO_WKR', aggfunc='sum'), CONSUMO_ANNUO=pd.NamedAgg(column='CONSUMO_ANNUO', aggfunc='sum')).reset_index()
    df_pp_pdr_aggr['ANNO_MESE'] = df_wkr['DATE'].astype(str).str.slice(start=0, stop=6)
    df_pp_pdr_aggr.to_csv(path_to_data + path_output + 'aggregato.csv')
    
    return (path_to_data + path_output)
