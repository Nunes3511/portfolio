import pandas as pd
import re
import json
import datetime as dt
import argparse
import sys

from api.delinea import delinea_get as dg
from datatest import validate
from pymongo import MongoClient
from random import randint
from google.oauth2 import service_account




#Função que puxa o horário de abertura e fechamento de loja formatado
def horario_loja(horario, mode):
    list_horarios = re.findall(r'\d+', horario) #trazendo apenas os números
    list_horarios = [number for number in list_horarios if len(number) == 2] #coletando apenas números com 2 digitos
    
    #Retornando abertura ou horário que a loja fecha
    if mode == "abertura":
        hora = list_horarios[0]+':'+ list_horarios[1]
    else:
        hora = list_horarios[2]+':'+ list_horarios[3]
    
    return hora

def msg_sing_plural(df, singular, plural): 
    #função para inclusão de mensagens que tenha decisões entre singular e plural
    if df == 1 :
        msg = '1 ' + singular
    else:
        msg = str(int(df)) + ' '+ plural
            
    return msg

def valide_input_string(string):
    #função que limpa o input de dados de string para que não tenha um injeção maléfica
    string = str(string)
    string = re.escape(string)
    string = string.replace('\t', '').replace('\n', '').replace('\r', '').replace('%', '').replace(',', '')
    string = string.strip() 
    return string

def strftime_format(format):
    def func(value):
        try:
            dt.datetime.strptime(value, format)
        except ValueError:
            return False
        return True
    func.__doc__ = f'should use date format {format}'
    return func

def mongo_connect(collection,dt_start,dt_end):
    client = MongoClient(dg.delinea_eva('mongodb-eva-read', 'uri'))
    
    #validações de input
    collection = valide_input_string(collection)
    validate(dt_start, strftime_format('%Y-%m-%d'))
    validate(dt_end, strftime_format('%Y-%m-%d'))

    db = client['evva']
    
    if collection == 'mcc_rpt_red':
        mongo_db = pd.DataFrame(list(db[collection].find({"dt_mrred":'%(dt_end)s' %{"dt_end": dt_end}, "cd_empgcb" : 21})))
    elif collection == 'mcc_rpt_red_lider':
        mongo_db = pd.DataFrame(list(db['mcc_rpt_red'].find({"dt_mrred": {"'$lt': %(dt_end)s, '$gte': %(dt_start)s"%{"dt_end": dt_end, "dt_start":dt_start}}, "cd_empgcb" : 21 })))
    elif collection == 'usr_sgr':
        mongo_db = pd.DataFrame(list(db[collection].find({"dt_usrsgr_fim_jor": '%(dt_end)s'%{"dt_end": dt_end}, "cd_empgcb" : 21})))
    elif collection == 'pam_hrr_adp':
        mongo_db = pd.DataFrame(list(db[collection].find({"cd_empgcb": 21})))
    elif collection == 'incentivo_docker':
        mongo_db = pd.DataFrame(list(db[collection].find({"empresa": '21'})))
    else:
        sys.exit()
    
    return mongo_db

def mongo_vo_connect(collection, mode, inicio, TODAY):
    
    #validações de input
    collection = valide_input_string(collection)
    validate(TODAY, strftime_format('%Y-%m-%d'))
    validate(inicio, strftime_format('%Y-%m-%d'))
    TODAY = pd.to_datetime(TODAY)
    inicio = pd.to_datetime(inicio)

    if isinstance(inicio, dt.datetime):
        TODAY = pd.to_datetime(TODAY)
    else:
        sys.exit()
    
    client = MongoClient(dg.delinea_eva('mongodb-eva-read', 'uri'))
    db = client['catalogo']

    if collection == 'atendimentos':
        if mode == 'vendedor':
            mongo_db = pd.DataFrame(list(db[collection].find({
                        "status": {"$in": ["EM_ANDAMENTO", "FINALIZADO"]},
                        "dataHoraCadastro": {'$gte': inicio, '$lte': TODAY}
        })))            
        elif mode == "gerente":
            #Buscando os atendimentos feitos no dia atual   
            mongo_db_cad = pd.DataFrame(list(db[collection].find({"dataHoraCadastro":{'$gte': inicio, '$lte': TODAY}})))
            mongo_db_ate_ini = pd.DataFrame(list(db[collection].find({"dataHoraInicioAtendimento":{'$gte': inicio, '$lte': TODAY}})))
            mongo_db_ate = pd.DataFrame(list(db[collection].find({"dataHoraFinalAtendimento":{'$gte': inicio, '$lte': TODAY}})))

            mongo_db = pd.concat([mongo_db_cad,mongo_db_ate_ini, mongo_db_ate]).reset_index()#juntando todas as base
            mongo_db = mongo_db.drop(columns = 'index') 
    
    return mongo_db

def download_base_pull(perfil):
    # PARSER
    description = 'Consolida rotinas de automatização da EVVA'

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument('-m',
                        metavar='modo',
                        type=str,
                        required=True,
                        help='escolha qual rotina rodar',
                        choices=['bom_dia', 'update_pull', 'resultados_push', 'resultados_pull'])


    # Get GBQ Project ID for connection
    GBQ_API_KEY_FILEPATH = './api/google_api/config.json'
    with open(GBQ_API_KEY_FILEPATH) as f:
        data = json.load(f)
    PROJECT_ID = data['project_id']

    CREDENTIALS = service_account.Credentials.from_service_account_file(GBQ_API_KEY_FILEPATH)

    # Nome das Tabelas no GBQ
    GBQ_TABLES = {
                'vendedor': 'base_pull_vendedor',
                'vendedor2': 'base_pull_vendedor2',
                'gerente': 'base_pull_gerente',
                'cal': 'base_pull_cal',
                'car': 'base_pull_car',
                'backoffice': 'base_pull_backoffice',
                'regional': 'base_pull_regional',
                'lojas_info': 'base_pull_lojas_info',
                'regs_info': 'base_pull_regs_info',
                'consultor' : 'base_pull_consultor',
                'contacts': 'base_pull_daily_contacts',
                'ponto_online':'base_pull_ponto_online',
                'users': 'base_pull_users',
                'status_via_app':'base_pull_status_via_app',
                'gestao_usuarios':'base_pull_gestao_usuarios',
                'username_userid':'username_userid',
                'roteirizacao':'roteirizacao',
                'extrato': 'base_pull_extrato',
            }

    #Downloada a base pull
    fixed_datas = ['username_userid', 'roteirizacao']

    if perfil in fixed_datas:
        base_pull = pd.read_gbq('SELECT * FROM fixed_data.%s' % GBQ_TABLES[perfil], PROJECT_ID, credentials = CREDENTIALS)
    else:
        base_pull = pd.read_gbq('SELECT * FROM customtables.%s' % GBQ_TABLES[perfil], PROJECT_ID, credentials = CREDENTIALS)
    
    return base_pull