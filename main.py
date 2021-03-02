import pandas as pd
import openpyxl
from pathlib import Path
from datetime import datetime
import numpy as np

#Declaração dos joins
dias_semana = ['seg','ter','qua','qui','sex','sáb','dom'] #Utilizar com o datetime data.weekday() para extrair o indice da data
fx_horario = {'Madrugada':'00 |-- 06','Manhã':'06 |-- 12','Tarde':'12 |-- 18','Noite':'18 |-- 24','Horario Desconhecido':'Horario Desconhecido'} #Utilizar com a coluna faixa de horário
meses = {1:'janeiro',2:'fevereiro',3:'marco',4:'abril',5:'maio',6:'junho',7:'julho',8:'agosto',9:'setembro',10:'outubro',11:'novembro',12:'dezembro'}
tipo_veiculo2 = {'MOTOCICLETA':'Motocicletas','MOTONETA':'Motocicletas','AUTOMOVEL':'Veículos Leves','CAMINHONETE':'Veículos Leves','UTILITARIO':'Veículos Leves','CAMINHAO':'Veículos Pesados','CAMINHAO TRATOR':'Veículos Pesados','CAMIONETA':'Veículos Leves','CICLOMOTOR':'Outros Veículos','MICROONIBUS':'Veículos Pesados','ONIBUS':'Veículos Pesados','PREJUDICADO':'Outros Veículos','REBOQUE':'Reboques','SEMI-REBOQUE':'Outros Veículos','SIDE-CAR':'Outros Veículos','TRICICLO':'Outros Veículos','VAN':'Veículos Leves','NAO IDENTIFICADO':'Não Identificado'}


#Dataframe inicializado
dt = pd.read_excel('teste.xlsx', header=0, engine='openpyxl') #OBS: ajustar o formato das datas e horas na hora da extração
dt['index'] = range(0, len(dt))
dt['dia_fato_siac_rf'] = ''
dt['mes_fato_siac_rf'] = ''
dt['ano_fato_siac_rf'] = ''
dt['mes_registro_siac_rf'] = ''
dt['faixa_hora_2'] = ''
dt['local_sisp_prec_siac'] = ''
dt['local_ocorrencia_siac_rf'] = ''
dt['regiao_siac_rf'] = ''
dt['risp_siac_rf'] = ''
dt['aisp_siac_rf'] = ''
dt['bairros_siac_rf'] = ''
dt['distritos'] = ''
dt['bairros_sisp_prec_siac'] = ''
dt['marca'] = ''
dt['modelo'] = ''
dt['tipo_de_veiculo_siac_1_rf'] = ''
dt['tipo_de_veiculo_siac_2_rf'] = ''

print(dt)

for iten in range(0,len(dt)): #Loop to add the column dia semana 
    data_fato = dt.at[iten, 'DATA FATO']
    index_data_fato = data_fato.weekday()
    dt.at[iten, 'dia_fato_siac_rf'] = dias_semana[index_data_fato]

for iten in range(0,len(dt)): #Loop to add the column fx_hora
    periodo = dt.at[iten, 'FAIXA DE HORA']
    dt.at[iten, 'faixa_hora_2'] = fx_horario[periodo]

for iten in range(0,len(dt)): #Loop to add the column mes_fato_siac_rf
    data_fato = dt.at[iten, 'DATA FATO']
    mes_index = data_fato.month
    dt.at[iten, 'mes_fato_siac_rf'] = meses[mes_index]

for iten in range(0,len(dt)): #Loop to add the column ano_fato_siac_rf
    data_fato = dt.at[iten, 'DATA FATO']
    dt.at[iten, 'ano_fato_siac_rf'] = data_fato.year

for iten in range(0,len(dt)): #Loop to add the column mes_registro_siac_rf
    data_registro = dt.at[iten, 'DATA REGISTRO']
    mes_reg_index = data_registro.month
    dt.at[iten, 'mes_registro_siac_rf'] = meses[mes_reg_index]

for iten in range(0,len(dt)): #Loop to add the column local_ocorrencia_siac_rf
    dt.at[iten, 'local_ocorrencia_siac_rf']= dt.at[iten, 'LOCAL OCORRENCIA']

for iten in range(0,len(dt)): #Loop to add the column bairros_siac_rf
    dt.at[iten, 'bairros_siac_rf'] = dt.at[iten, 'BAIRRO OCORRENCIA']


#PROC DO PREC
dt_local = pd.read_excel('./src/localdetran.xlsx', header=0, engine='openpyxl', index_col=0)
dt_regiao = pd.read_excel('./src/regiaodetran.xlsx', header=0, engine='openpyxl', index_col=0)
dt_risp = pd.read_excel('./src/rispdetran.xlsx', header=0, engine='openpyxl', index_col=0)
dt_aisp = pd.read_excel('./src/aispdetran.xlsx', header=0, engine='openpyxl', index_col=0)
dt_bairro = pd.read_excel('./src/bairrosdetran.xlsx', header=0, engine='openpyxl', index_col=0)
dt_veiculo_tipo = pd.read_excel('./src/tipoveiculo.xlsx', header=0, engine='openpyxl',index_col=0)

dt = dt.join(dt_local, on='local_ocorrencia_siac_rf')
#dt = dt.drop(columns=['MUNICIPIO DETRAN'])
dt = dt.join(dt_regiao, on='local_ocorrencia_siac_rf')
#dt = dt.drop(columns=['MUNICIPIO'])
dt = dt.join(dt_risp, on='local_ocorrencia_siac_rf')
#dt = dt.drop(columns=['RISPs'])
dt = dt.join(dt_aisp, on='bairros_siac_rf')
#dt = dt.drop(columns=['AISPs'])
dt = dt.join(dt_bairro, on='bairros_siac_rf')

dt['local_sisp_prec_siac'] = dt['MUNICIPIO SISP']
dt['regiao_siac_rf'] = dt['REGIÃO']
dt['risp_siac_rf'] = dt['RISPs']
dt['aisp_siac_rf'] = dt['AISPs']
dt['bairros_sisp_prec_siac'] = dt['BAIRROS SISP']

#Preparação do Veiculo

dt[['marca','modelo']] = dt['MARCA/MODELO'].str.split('/',1,expand=True)

for iten in range(0,len(dt)): #Loop to add the column tipo_de_veiculo_siac_1_rf
    dt['tipo_de_veiculo_siac_1_rf'] = dt['TIPO DE VEICULO']

dt = dt.join(dt_veiculo_tipo, on='tipo_de_veiculo_siac_1_rf')
dt['tipo_de_veiculo_siac_2_rf'] = dt['Tipo Veiculo']

with pd.ExcelWriter('resultado_teste.xlsx',engine='xlsxwriter', datetime_format='dd/mm/yyyy',date_format='dd/mm/yyyy') as writer:
            dt.to_excel(writer,index=False)









