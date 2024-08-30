import pyodbc
import pandas as pd
import pandas.io.sql as psql
import api.delinea.delinea_get as dg
import configparser

SQL_TABLE_PAINEL_MEU_TIME_LOJA_ACUM = """SELECT DISTINCT
                coalesce(MERC.cdMatricula, SERV.cdMatricula) + 2100000000 as username,
                coalesce(MERC.cdMatricula, SERV.cdMatricula) as matricula,
                FUNC.NmFuncionario as nome,
                coalesce(MERC.cdFilial, SERV.cdFilial) as filial,
                coalesce(MERC.cdDiretoria, SERV.cdDiretoria) as diretoria,
                coalesce(MERC.cdRegional, SERV.cdRegional) as regional,
                MERC.flag_meta_merc,
                MERC.perc_merc_acum,
                MERC.perc_cdc_acum,
                SERV.perc_serv_acum,
                CASE 
                    WHEN MERC.flag_meta_merc = 0 THEN 'Sem meta'
                    WHEN MERC.perc_merc_acum >= 100 AND MERC.perc_cdc_acum >= 100 AND SERV.perc_serv_acum >= 100 THEN 'Reconheça' 
                    WHEN MERC.perc_merc_acum < 100 AND MERC.perc_cdc_acum < 100 AND SERV.perc_serv_acum < 100 THEN 'Analise'
                    ELSE 'Incentive'
                END AS status,
                CASE 
                    WHEN MERC.flag_meta_merc = 0 THEN 'Colaborador não possui meta.'
                    WHEN MERC.perc_merc_acum >= 100 AND MERC.perc_cdc_acum >= 100 AND SERV.perc_serv_acum >= 100 THEN 'Atingindo Mercantil, CDC e S&S'
                    WHEN MERC.perc_merc_acum >= 100 AND MERC.perc_cdc_acum >= 100 AND SERV.perc_serv_acum < 100 THEN 'Atingindo Mercantil e CDC'
                    WHEN MERC.perc_merc_acum >= 100 AND MERC.perc_cdc_acum < 100 AND SERV.perc_serv_acum >= 100 THEN 'Atingindo Mercantil e S&S'
                    WHEN MERC.perc_merc_acum >= 100 AND MERC.perc_cdc_acum < 100 AND SERV.perc_serv_acum < 100 THEN 'Atingindo Mercantil'
                    WHEN MERC.perc_merc_acum < 100 AND MERC.perc_cdc_acum >= 100 AND SERV.perc_serv_acum >= 100 THEN 'Atingindo CDC e S&S'
                    WHEN MERC.perc_merc_acum < 100 AND MERC.perc_cdc_acum >= 100 AND SERV.perc_serv_acum < 100 THEN 'Atingindo CDC'
                    WHEN MERC.perc_merc_acum < 100 AND MERC.perc_cdc_acum < 100 AND SERV.perc_serv_acum >= 100 THEN 'Atingindo S&S'
                    ELSE 'Não atingiu nenhum indicador'
                END AS metrica,
                dateadd(hour,-3,GETDATE()) as dt_atualizacao,
                INDICADORES.cod_10_digitos_vendedor, 
                INDICADORES.total_leads_mes,
                INDICADORES.total_leads_semana,
                INDICADORES.total_leads_semana_prev,
                INDICADORES.periodo_leads_semana_prev,                
                INDICADORES.total_leads_ontem,                
                INDICADORES.leads_posicao_mes,
                INDICADORES.leads_posicao_semana,
                INDICADORES.total_vendas_vo_mes, 
                INDICADORES.total_vendas_vo_semana,
                INDICADORES.total_vendas_vo_ontem,
                INDICADORES.vendas_vo_posicao_semana,
                INDICADORES.vendas_vo_posicao_mes,
                INDICADORES.total_viamais_mes,
                INDICADORES.total_viamais_semana,
                INDICADORES.total_viamais_semana_prev,
                INDICADORES.periodo_viamais_semana_prev,
                INDICADORES.total_viamais_ontem,
                INDICADORES.posicao_viamais_semana,
                INDICADORES.posicao_viamais_mes,                
                INDICADORES.total_zap_mes,
                INDICADORES.total_zap_semana,
                INDICADORES.total_zap_ontem,
                INDICADORES.posicao_zap_semana,
                INDICADORES.posicao_zap_mes,
                INDICADORES.total_app_dois_mes,
                INDICADORES.total_app_dois_semana,
                INDICADORES.total_app_dois_ontem,
                INDICADORES.total_app_dois_semana_prev,
                INDICADORES.periodo_app_dois_semana_prev,
                INDICADORES.posicao_app_dois_semana,
                (INDICADORES.total_viamais_mes + INDICADORES.total_leads_mes) AS tt_clientes_atendidos_vo_acum
            FROM (
                SELECT 
                    cdMatricula,
                    cdFilial,
                    cdDiretoria,
                    cdRegional,
                    CAST(CASE WHEN SUM(vlMercantilMeta) = 0 THEN 0 ELSE (SUM(vlMercantilReal) / SUM(vlMercantilMeta)) * 100 END AS FLOAT) as perc_merc_acum,
                    CAST(CASE WHEN SUM(vlCdcMeta) = 0 THEN 0 ELSE (SUM(vlCdcReal) / SUM(vlCdcMeta)) * 100 END  AS FLOAT) as perc_cdc_acum,
                    CASE WHEN SUM(vlMercantilMeta) = 0 THEN 0 ELSE 1 END AS flag_meta_merc
                FROM plataf_oploja.TB_EFI_VND_MCR_MES 
                WHERE dtAno = YEAR(GETDATE()-1) AND dtMes = MONTH(GETDATE()-1) AND dtDia <= DAY(GETDATE()-1)
                GROUP BY cdMatricula, cdFilial, cdDiretoria, cdRegional
            ) AS MERC  
            FULL JOIN (
                SELECT 
                    cdMatricula,
                    cdFilial,
                    cdDiretoria,
                    cdRegional,
                    CAST(CASE WHEN SUM(vlServicoMeta) = 0 THEN 0 ELSE (SUM(vlServicoReal) / SUM(vlServicoMeta)) * 100 END AS FLOAT) as perc_serv_acum
                FROM plataf_oploja.TB_EFI_VND_SERV_MES 
                WHERE dtAno = YEAR(GETDATE()-1) AND dtMes = MONTH(GETDATE()-1) AND dtDia <= DAY(GETDATE()-1)
                GROUP BY cdMatricula, cdFilial, cdDiretoria, cdRegional
            ) AS SERV
            ON MERC.cdMatricula = SERV.cdMatricula AND MERC.cdFilial = SERV.cdFilial AND MERC.cdDiretoria = SERV.cdDiretoria AND MERC.cdRegional = SERV.cdRegional
            INNER JOIN (
                SELECT
                    CdMatricula,
                    CASE
                        WHEN CHARINDEX(' ', NmFuncionario) > 0 THEN CONCAT(
                            SUBSTRING(NmFuncionario, 1, CHARINDEX(' ', NmFuncionario) - 1),
                            ' ',
                            UPPER(SUBSTRING(
                                SUBSTRING(NmFuncionario, CHARINDEX(' ', NmFuncionario) + 1, LEN(NmFuncionario)),
                                1,
                                1
                            ))
                        )
                        ELSE NmFuncionario
                    END AS NmFuncionario
                FROM plataf_oploja.TB_FUNCIONARIO
                WHERE DsCargo LIKE '%VENDEDOR%' OR DsCargo LIKE '%ASSESSOR DE VENDAS%' AND DsCargo NOT LIKE '%APRENDIZ%' AND CdEmpresa = 21
            ) AS FUNC
            ON COALESCE(MERC.cdMatricula, SERV.cdMatricula) = FUNC.cdMatricula
            LEFT JOIN (
                SELECT
                    vendedor as cdMatricula,
                    cod_10_digitos_vendedor, 
                    total_leads_mes,
                    total_leads_semana,
                    total_leads_semana_prev,
                    periodo_leads_semana_prev,
                    total_leads_ontem,
                    leads_posicao_semana,
                    leads_posicao_mes,
                    total_vendas_vo_mes, 
                    total_vendas_vo_semana,
                    total_vendas_vo_ontem,
                    vendas_vo_posicao_semana,
                    vendas_vo_posicao_mes,
                    total_viamais_mes,
                    total_viamais_semana,
                    total_viamais_semana_prev,
                    periodo_viamais_semana_prev,
                    total_viamais_ontem,
                    posicao_viamais_semana,
                    posicao_viamais_mes,
                    total_zap_mes,
                    total_zap_semana,
                    total_zap_ontem,
                    posicao_zap_semana,
                    posicao_zap_mes,
                    total_app_dois_mes,
                    total_app_dois_semana,
                    total_app_dois_ontem,
                    total_app_dois_semana_prev,
                    periodo_app_dois_semana_prev,
                    posicao_app_dois_semana
                FROM plataf_oploja.tb_painel_indicadores_vo_vendedor
            ) AS INDICADORES
            ON COALESCE(MERC.cdMatricula, SERV.cdMatricula) = INDICADORES.cdMatricula
            ORDER BY status ASC
            """
            
SQL_TABLE_PAINEL_CATEGORIA_DIA = """SELECT 
                    CAT.cdFilial as filial,
                    CAT.cdDiretoria as diretoria,
                    CAT.cdRegional as regional,
                    CAT.dsSubCategoria as categoria,
                    CAT.meta_dia,
                    CAT.real_off_dia,
                    CAT.real_on_dia,
                    dateadd(hour,-3,GETDATE()) as dt_atualizacao
                FROM (
                    SELECT 
                    cdFilial,
                    cdDiretoria,
                    cdRegional,
                    dsSubCategoria,
                    CAST(SUM(vlMercantilMeta) AS FLOAT) as meta_dia,
                    CAST(SUM(vlMercantilRealLoja) AS FLOAT) as real_off_dia,
                    CAST(SUM(vlMercantilRealVo) AS FLOAT) as real_on_dia
                    FROM plataf_oploja.TB_EFI_LOJA_MCR_MES_FLASH_LANDING
                    GROUP BY cdFilial, cdDiretoria, cdRegional, dsSubCategoria)  AS CAT 
                """
            
PAINEL_QUERIES = {
                'categoria_loja_dia': SQL_TABLE_PAINEL_CATEGORIA_DIA,
                'meu_time_loja_acum': SQL_TABLE_PAINEL_MEU_TIME_LOJA_ACUM,
            }
            
def connect_database(): 
    config = configparser.ConfigParser()
    config.read('view_painel.ini')
  
    db_server = config.get("database", "SERVER")
    db_database = config.get("database", "DATABASE")
    db_user = config.get("database", "UID")
    db_password = dg.delinea_eva('sql-server', 'PWD')
  
    conn = pyodbc.connect(driver='{ODBC Driver 18 for SQL Server}', 
                          server=db_server, database=db_database, 
                          user=db_user, password=db_password)
    
    return conn

def get_update_painel_tables(QUERY):

    #Gerando a conexão  
    conn = connect_database()

    #LISTAR DADOS EM DATAFRAME
    df = pd.read_sql(PAINEL_QUERIES[QUERY], con = conn )

    return df