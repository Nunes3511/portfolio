# POC - Me chama no Zap

#Alertas para os vendedores  sobre os clientes disponiveis para atendimento
#Extrato de atendimentos para os gerentes

#📅 Envio: Diário

#***Requirements:***

# - MongoDB VO
# - MongoDB Evva
# - Base users
# - Base de roteirização

#-----------IMPORTS-----------
import pandas as pd
import numpy as np
import datetime as dt
import locale

from utils import horario_loja, msg_sing_plural, mongo_vo_connect, mongo_connect, download_base_pull
locale.setlocale(locale.LC_ALL, 'Portuguese')

DT_TODAY = dt.datetime.today()
DT_TODAY_WEEKDAY = DT_TODAY.weekday() # Segunda = 0, Terça = 1, Quarta = 2, Quinta = 3, Sexta = 4, Sábado = 5, Domingo = 6

NOW = dt.datetime.now()
NOW_HR = NOW.strftime('%H:%M')

## ----------TEMPLATES------------

TEMPLATE_VENDEDOR = """📣 Me chama no zap, {clientes} na fila aguardando atendimento! 

*Caso a fila esteja vazia, o atendimento já foi iniciado por outro vendedor.

#EventoMechamanozap"""

TEMPLATE_VENDEDOR_PONTO = """📣 Chama no contatinho, {clientes} na fila aguardando atendimento! 

*Caso a fila esteja vazia, o atendimento já foi iniciado por outro vendedor.

#EventoChamanocontatinho"""

# --- Gerente---
TEMPLATE_GERENTE =  """📣 Me chama no zap, {aguardando} na fila aguardando atendimento! 

#ResultadosMechamanozap"""

TEMPLATE_GERENTE_PONTO =  """📣 Chama no contatinho, {aguardando} na fila aguardando atendimento!

#ResultadosChamanocontatinho"""

TEMPLATE_EXTRATO = """
📱Total de atendimentos: {tt_atendimentos}
▪ Iniciado: {iniciado} ({perc_iniciado}%)
▪ Em andamento: {em_andamento} ({perc_em_andamento}%)
▪ Finalizado: {finalizado} ({perc_finalizado}%)
"""

loja_com_fila = "\n\nJá enviei para os seus vendedores um alerta sobre os clientes na fila de atendimento. 😉"
loja_sem_fila = "\n\nTodos os seus clientes até {} já foram atendidos! 💜".format(NOW_HR)


def create_message_me_chama_no_zap(DT_TODAY):
    
    DT_DAY = DT_TODAY.strftime('%Y-%m-%d')

    #---------Data----------
    #Base de roteirização pra pegar horário de funcionamento das lojas
    df_roteirizacao = pd.read_csv('./fixed_data/roteirizacao.csv', encoding = 'utf-8', sep = ';', usecols=['CD_FIL','HORÁRIO DE ABERTURA SEG A SEX', 'HORÁRIO DE ABERTURA SÁB', 'HORÁRIO DE ABERTURA DOM', 'CD_FUN_GER', 'NOME_GERENTE_LOJA', 'BAND'])

    #Base de ids Skore
    df_user_id = download_base_pull('username_userid')
    df_user_id = df_user_id[['id', 'username']]

    #Base de cargos
    df_cargos = download_base_pull("status_via_app")
    

    #Conexão mongo VO
    df_atendimentos = mongo_vo_connect("atendimentos",  "vendedor", '')

    #Base do ponto
    df_mongo = mongo_connect('mcc_rpt_red', DT_DAY, DT_DAY)

    #filiais do piloto
    filiais_rollout = pd.read_excel('./fixed_data/me_chama_no_zap_rollout_sem_notificação.xlsx')


    #------- Prepare data -------

    # ---- Gerando a base de clientes recentes --------
    #Gerando a base de filiais com clientes recentes
    df_filiais_com_clientes_recentes = df_atendimentos[['dataHoraCadastro', 'filial']]

    #colocando no fuso horário do Brasil
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'] - dt.timedelta(hours=3)

    #Filtrando apenas a data atual
    df_filiais_com_clientes_recentes['dia'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].apply(lambda x: str(x).split(' ')[0])
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes[df_filiais_com_clientes_recentes['dia'] == DT_DAY]

    #Gerando a diferença entre horas
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].apply(lambda x:DT_TODAY - x)

    #Filtrando apenas quem tem clientes recentes
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].astype(str)
    df_filiais_com_clientes_recentes['dif_min'] = df_filiais_com_clientes_recentes.dataHoraCadastro.apply(lambda x: int(x.split(':')[1]))
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes[df_filiais_com_clientes_recentes['dif_min'] <= 3]

    #Criando a lista
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes['filial'].copy()
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes.drop_duplicates()
    filiais_com_clientes_recentes = df_filiais_com_clientes_recentes.tolist()

    #------Loja--------

    #Gerando a base de lojas disponiveis
    df_lojas_disponiveis = df_roteirizacao.copy()

    #Seleção da coluna pra comparativo do horário
    if DT_TODAY_WEEKDAY <= 4 :
        coluna = "HORÁRIO DE ABERTURA SEG A SEX"
    elif DT_TODAY_WEEKDAY == 6 :
        coluna = "HORÁRIO DE ABERTURA DOM"
    else:
        coluna = "HORÁRIO DE ABERTURA SÁB"

    #Retirando as lojas fechadas
    df_lojas_disponiveis = df_lojas_disponiveis[df_lojas_disponiveis[coluna].str.contains("FECHADA")==False]
    #Tratando e trazendo os horários de abertura e fechamento da loja
    df_lojas_disponiveis = df_lojas_disponiveis.dropna()
    df_lojas_disponiveis["abertura"] = df_lojas_disponiveis[coluna].apply(lambda x: horario_loja(x, "abertura"))
    df_lojas_disponiveis["fechamento"] = df_lojas_disponiveis[coluna].apply(lambda x: horario_loja(x, "fechamento"))

    #Verificando se a loja está aberta
    df_lojas_disponiveis["flag_loja_aberta"] = np.where((NOW_HR <= df_lojas_disponiveis["fechamento"]) & (NOW_HR >= df_lojas_disponiveis["abertura"]), True, False)
    listlojasdisponiveis = df_lojas_disponiveis[df_lojas_disponiveis['flag_loja_aberta'] == True]

    #Trazendo apenas as lojas do rollout
    #listlojasdisponiveis = listlojasdisponiveis[~listlojasdisponiveis['CD_FIL'].isin(filiais_rollout['Filiais:'])]

    #------Colabs--------
    #gerando a base de usuários disponiveis para receber o alerta
    #verificando a quantidade de marcações dos usuários
    df_disponiveis = df_mongo.groupby(['cd_fun', 'cd_fil_rlg']).hr_mrred.count().reset_index()

    #Filtrando as lojas disponiveis
    df_disponiveis = df_disponiveis[df_disponiveis['cd_fil_rlg'].isin(listlojasdisponiveis['CD_FIL'])]
    #Verificando se a quantidade de marcações é impar, pois isso determina que o usuário está ativo em loja
    df_disponiveis['flag_disponivel'] = df_disponiveis['hr_mrred']%2 > 0
    df_disponiveis = df_disponiveis[df_disponiveis['flag_disponivel'] == True]
    #Filtrando os vendedores
    df_cargos = df_cargos[['matricula', 'Nivel_2']]
    df_cargos['matricula'] = df_cargos['matricula'].astype(int)
    df_cargos = df_cargos[df_cargos['matricula'].isin(df_disponiveis['cd_fun'])]
    df_cargos = df_cargos[df_cargos['Nivel_2'] == 'Vendedor']
    df_disponiveis = df_disponiveis[df_disponiveis['cd_fun'].isin(df_cargos['matricula'])]

    #------Clientes--------
    #Gerando a base de clientes esperando atendimentos

    #Filtrando os clientes aguardando atendimento
    df_atendimentos_n_iniciados = df_atendimentos[df_atendimentos['status'] == "NAO_INICIADO"] #somente atendimentos não iniciados
    df_atendimentos_n_iniciados = df_atendimentos_n_iniciados.groupby(['filial']).nome.count().reset_index() #contando os clientes
    df_atendimentos_n_iniciados = df_atendimentos_n_iniciados.rename(columns={'nome':'qtd_clientes'}) #renomeando a coluna
    #Trazendo os colaboradores de cada filial para receber o alerta
    df_message = df_disponiveis.merge(df_atendimentos_n_iniciados, left_on = 'cd_fil_rlg', right_on = 'filial', how = 'inner')

    #Trazendo a bandeira do vendedor

    df_bandeira = df_roteirizacao[['CD_FIL', 'BAND']].copy()
    df_message = df_message.merge(df_roteirizacao, left_on = 'cd_fil_rlg', right_on = 'CD_FIL', how = 'left')

    #---------------Message-----------

    #Aplicando o modelo da mensagem no código
    df_message['message'] = ''

    for fun in df_message.itertuples():
        format_dict = {
            'clientes' : msg_sing_plural(fun.qtd_clientes, "cliente", "clientes"),
        }
    
        df_message.at[fun.Index, 'message'] = TEMPLATE_VENDEDOR.format(**format_dict) if fun.BAND == "CB" else TEMPLATE_VENDEDOR_PONTO.format(**format_dict)
    ## ----------Padrão Skore + clientes_recentes-----------
    
    #alterei
    #df_message['flag_clientes_recentes'] = df_message['cd_fil_rlg'] in filiais_com_clientes_recentes
    df_message['flag_clientes_recentes'] = df_message['cd_fil_rlg'].isin(filiais_com_clientes_recentes)


    #trazendo o id skore pra base
    df_message['username'] = df_message['cd_fun'] + 2100000000
    df_message['username'] = df_message['username'].astype(str)
    df_message['username'] = df_message['username'].apply(lambda x: x.replace('.0', ''))

    df_message = df_message.merge(df_user_id, on = 'username', how = 'inner')
    df_message = df_message.rename(columns = {'id':'user_id'})

    #Formatando a base conforme padrão Skore

    df_send = df_message[['user_id', 'message', 'flag_clientes_recentes']]

    df_send = df_send.dropna()
    df_send = df_send.reset_index()

    df_send['user_id'] = df_send['user_id'].astype(int)
    df_send['type'] = 'message'
    df_send['image_url'] = ''
    df_send['send_at'] = ''
    df_send['text'] = ''
    df_send['intent'] = ''

    return df_send


def create_me_chama_no_zap_gl(DT_TODAY, TODAY, DIA, MONTH_STR):

    start = pd.to_datetime(TODAY)

    #---------Data----------
    #Base de roteirização pra pegar horário de funcionamento das lojas
    df_roteirizacao = pd.read_csv('./fixed_data/roteirizacao.csv', encoding = 'utf-8', sep = ';', usecols=['CD_FIL','HORÁRIO DE ABERTURA SEG A SEX', 'HORÁRIO DE ABERTURA SÁB', 'HORÁRIO DE ABERTURA DOM', 'CD_FUN_GER', 'NOME_GERENTE_LOJA', 'BAND'])

    #Alteração
    #Base de ids Skore
    df_user_id = download_base_pull('username_userid')
    df_id = df_user_id[['id', 'username', 'team']].copy()

    #Conexão mongo VO
    df_atendimentos = mongo_vo_connect("atendimentos",  "gerente", start)

    #filiais do piloto
    filiais_rollout = pd.read_excel('./fixed_data/me_chama_no_zap_rollout.xlsx')

    #-------Data prep------
    #alterei
    DT_DAY = DT_TODAY.strftime('%Y-%m-%d')

    # ---- Gerando a base de clientes recentes --------
    #Gerando a base de filiais com clientes recentes
    df_filiais_com_clientes_recentes = df_atendimentos[['dataHoraCadastro', 'filial']]

    #colocando no fuso horário do Brasil
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'] - dt.timedelta(hours=3)

    #Filtrando apenas a data atual
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes.dropna()
    df_filiais_com_clientes_recentes['dia'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].apply(lambda x: str(x).split(' ')[0])
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes[df_filiais_com_clientes_recentes['dia'] == DT_DAY]

    #Gerando a diferença entre horas
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].apply(lambda x:DT_TODAY - x)

    #Filtrando apenas quem tem clientes recentes
    df_filiais_com_clientes_recentes['dataHoraCadastro'] = df_filiais_com_clientes_recentes['dataHoraCadastro'].astype(str)
    df_filiais_com_clientes_recentes['dif_min'] = df_filiais_com_clientes_recentes.dataHoraCadastro.apply(lambda x: int(x.split(':')[1]))
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes[df_filiais_com_clientes_recentes['dif_min'] <= 3]

    #Criando a lista
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes['filial'].copy()
    df_filiais_com_clientes_recentes = df_filiais_com_clientes_recentes.drop_duplicates()
    filiais_com_clientes_recentes = df_filiais_com_clientes_recentes.tolist()

    #--------- lojas ----------
    #Gerando a base de lojas disponiveis
    df_lojas_disponiveis = df_roteirizacao.copy()

    #Seleção da coluna pra comparativo do horário
    if DT_TODAY_WEEKDAY <= 4 :
        coluna = "HORÁRIO DE ABERTURA SEG A SEX"
    elif DT_TODAY_WEEKDAY == 6 :
        coluna = "HORÁRIO DE ABERTURA DOM"
    else:
        coluna = "HORÁRIO DE ABERTURA SÁB"

    #Retirando as lojas fechadas
    df_lojas_disponiveis = df_lojas_disponiveis[df_lojas_disponiveis[coluna].str.contains("FECHADA")==False]

    #Tratando e trazendo os horários de abertura e fechamento da loja
    df_lojas_disponiveis["abertura"] = df_lojas_disponiveis[coluna].apply(lambda x: horario_loja(x, "abertura"))
    df_lojas_disponiveis = df_lojas_disponiveis.dropna() 
    df_lojas_disponiveis["fechamento"] = df_lojas_disponiveis[coluna].apply(lambda x: horario_loja(x, "fechamento"))

    #Verificando se a loja está aberta
    df_lojas_disponiveis["flag_loja_aberta"] = np.where((NOW_HR <= df_lojas_disponiveis["fechamento"]) & (NOW_HR >= df_lojas_disponiveis["abertura"]), True, False)

    #Trazendo apenas as lojas do rollout
    #df_lojas_disponiveis = df_lojas_disponiveis[df_lojas_disponiveis['CD_FIL'].isin(filiais_rollout['Filiais'])]

    #---------atendimentos-------
    #Trazendo a quantidade de atendimentos em cada status
    df_atendimentos_ger = pd.pivot_table(df_atendimentos, index = 'filial', columns = 'status', aggfunc='count', values = '_id').reset_index()

    #quantidade total de atendimentos iniciados loja 
    df_atendimentos_ini = df_atendimentos[df_atendimentos['status'] != "NAO_INICIADO"] #Tirando os atendimentos não iniciados
    df_atendimentos_count = df_atendimentos_ini.groupby(['filial']).status.count().reset_index()
    df_atendimentos_count = df_atendimentos_count.rename(columns = {'status':'total_atendimentos_ini'})

    #Trazendo a base com o valor de cada status e o total
    df_atendimentos_ger = df_atendimentos_ger.merge(df_atendimentos_count, on = 'filial', how = 'left')
    df_atendimentos_ger = df_atendimentos_ger.replace(np.nan, 0)

    #Incluindo todos os status dentro da tabela
    status = ['INICIADO', 'EM_ANDAMENTO', 'NAO_INICIADO', 'FINALIZADO']

    for s in status:
        if s not in df_atendimentos_ger.columns:
            df_atendimentos_ger[s] = 0


    #----------Message----------

    #Aplicando o extrato na base

    #Trazendo os dados do gerente para a base
    df_message = df_atendimentos_ger.merge(df_lojas_disponiveis[['CD_FIL', 'CD_FUN_GER', 'NOME_GERENTE_LOJA', 'flag_loja_aberta', 'BAND']], left_on = 'filial', right_on = 'CD_FIL', how = 'inner')

    #tratando o nome do gerente
    df_message = df_message.dropna(subset = ['NOME_GERENTE_LOJA'])
    df_message['NOME_GERENTE_LOJA'] = df_message['NOME_GERENTE_LOJA'].apply(lambda x: x.split(' ')[0].capitalize())


    df_message = df_message.dropna(subset=['filial'])

    #Aplicando a mensagem curta na base

    df_message['message'] = ''

    for fun in df_message.itertuples():
        if fun.NAO_INICIADO > 0:
            format_dict = {
                    'aguardando': msg_sing_plural(fun.NAO_INICIADO, 'cliente', 'clientes') if fun.NAO_INICIADO > 0 else ''
            }
            df_message.at[fun.Index, 'message'] = TEMPLATE_GERENTE.format(**format_dict) if fun.BAND == "CB" else TEMPLATE_GERENTE_PONTO.format(**format_dict)
        else:   
            ''
    
    df_message = df_message[df_message['message'] != '']
    df_message['flag_clientes_recentes'] = df_message['CD_FIL'].isin(filiais_com_clientes_recentes)

    #---------Formato Skore + flag loja aberta--------

    #trazendo o id skore pra base
    df_message['username'] = df_message['CD_FUN_GER'] + 2100000000
    df_message['username'] = df_message['username'].astype(str)
    df_message['username'] = df_message['username'].apply(lambda x: x.replace('.0', ''))

    df_message = df_message.merge(df_user_id, on = 'username', how = 'inner')
    df_message = df_message.rename(columns = {'id':'user_id'})

    #Formatando a base conforme padrão Skore + flag loja aberta

    df_send = df_message[['user_id', 'message', 'flag_loja_aberta', 'flag_clientes_recentes']]

    df_send = df_send.dropna()
    df_send = df_send.reset_index()
    df_send = df_send[['user_id', 'message', 'flag_loja_aberta', 'flag_clientes_recentes']]

    df_send['user_id'] = df_send['user_id'].astype(int)
    df_send['type'] = 'message'
    df_send['image_url'] = ''
    df_send['send_at'] = ''
    df_send['text'] = ''
    df_send['intent'] = ''

    #------------Espelhamento-----------

    #Trazendo os usuários do time de obrigatórios
    df_id = df_id.dropna()
    df = df_id[df_id['team'].str.contains('#Obrigatórios')]

    #trazendo as mensagens diferentes para cada espelho de acordo com a quantidade de usuários
    df_esp = df_send[df_send.index < df.id.count()]
    df_esp = df_esp.drop(columns = 'user_id')

    #tratando os dados do espelhamento
    obr_df = pd.DataFrame(df['id'].astype(int))
    obr_df = obr_df.rename(columns = {'id': 'user_id'})
    obr_df = obr_df.reset_index()
    obr_df = obr_df.drop(columns = 'index')

    #Trazendo as mensagens para a base principal
    obr_df = pd.concat([obr_df, df_esp], axis = 1) #Colunas
    df_tabela_envio = pd.concat([obr_df, df_send], axis = 0) #linhas

    return df_tabela_envio