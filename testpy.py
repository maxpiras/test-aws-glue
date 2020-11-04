import pandas as pd 
import datetime

def main():
    tms = datetime.datetime.now().strftime("%D%H:%M:%S")

    path_to_data = "s3://zus-qa-s3/algoritmo1"
    df_coef_res = pd.read_csv(path_to_data+'/profili_elaborati.csv')
    df_wkr = pd.read_csv(path_to_data+'/wkr.csv')
    df_rcu = pd.read_csv(path_to_data+'/rcu.csv')
    df_anagrafica_osservatori = pd.read_csv(path_to_data+'/anagrafica_osservatori.csv')

    df_rcu['station'] = df_rcu['station'].astype(str)
    df_pp_pdr = df_coef_res.merge(df_rcu,on='profilo').merge(df_anagrafica_osservatori,on='station',how='left') \
        .merge(df_wkr,on=['date','osservatorio'],how='left')
    df_pp_pdr = df_pp_pdr.where(~ df_pp_pdr['wkr'].isnull(),1)
    df_pp_pdr = df_pp_pdr.assign(k=df_pp_pdr['c_wkr']*df_pp_pdr['wkr']+df_pp_pdr['c_const'])
    df_pp_pdr = df_pp_pdr.assign(smc=df_pp_pdr['k']*df_pp_pdr['consumo_annuo']/100)

    df_pp_pdr.to_csv(path_to_data+'/output_pdr_'+tms+'.csv')
