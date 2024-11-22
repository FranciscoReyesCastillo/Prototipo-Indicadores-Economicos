import ast
import json
import mysql
import mysql.connector
import requests
from datetime import datetime, timedelta
from db_connection import get_connection
from envio_correo import envio_correo
import logging

logging.basicConfig(level = logging.DEBUG,
                    format = '%(asctime)s - %(levelname)s - %(message)s',
                    filename = 'indicadores_economicos.log',
                    encoding='utf-8',
                    filemode = 'a')


def obtener_rango_fechas(inicio, termino):
    try:
        dia_inicio = inicio.split('/')
        dia_inicio = int(dia_inicio[2])
        fecha_inicio = datetime(datetime.now().year, datetime.now().month, dia_inicio)
        if datetime.now().date() == fecha_inicio.date(): 
            termino_aux = termino.split('/')
            mes_termino =termino_aux[1]
            dia_termino =termino_aux[2]
            if "+" in mes_termino:
                fecha_hoy = datetime.now()
                if fecha_hoy.month == 12:
                    primer_dia_proximo_mes = datetime(fecha_hoy.year + 1, 1, 1)
                else:
                    primer_dia_proximo_mes = datetime(fecha_hoy.year, fecha_hoy.month + 1, 1)
                dia_termino = dia_termino.replace('%', '').strip('()')
                signo = dia_termino[0]
                dias_a_sumar = int(dia_termino[1:])
                if signo == "+":
                    fecha_termino = primer_dia_proximo_mes + timedelta(days=dias_a_sumar - 1)
                elif signo == "-":
                    fecha_termino = primer_dia_proximo_mes - timedelta(days=dias_a_sumar)
                fecha_inicio = fecha_inicio.strftime("%Y-%m-%d")
                fecha_termino = fecha_termino.strftime("%Y-%m-%d")
                return fecha_inicio, fecha_termino
        else:
            return None, None
    except Exception as err:
        logging.error(f"Error obtener_rango_fechas(): {err}")

def obtener_valor_dia_anterior_db(codigo):
    try:
        connection = get_connection()

        if connection:
            logging.info('Conexión correcta a base de datos.')
            cursor = connection.cursor()
            query_valor_dia_anterior = f"SELECT v.valor FROM valores_indicadores v INNER JOIN indicador_economico ie on v.indicador_id = ie.indicador_id WHERE ie.codigo = '{codigo}' and v.fecha = CURDATE() - INTERVAL 1 DAY;"
            cursor.execute(query_valor_dia_anterior)
            dia_anterior_valor = cursor.fetchone()
            return dia_anterior_valor
        else:
            logging.info('No se pudo conectar a la base de datos.')
    except mysql.connector.Error as err:
        logging.error(f"Error obtener_valor_dia_anterior_db(): {err}")
    finally:
        cursor.close()
        connection.close()

def insertar_valor_db(fecha,valor,codigo):
    try:
        connection = get_connection()

        if connection:
            logging.info('Conexión correcta a base de datos.')
            cursor = connection.cursor()
            query_insert = f"INSERT IGNORE INTO valores_indicadores (fecha, valor, indicador_id) VALUES ('{fecha}', '{valor}', (SELECT indicador_id FROM indicador_economico WHERE codigo = '{codigo}'));"
            cursor.execute(query_insert)
            connection.commit()
            return True
        else:
            logging.info('No se pudo conectar a la base de datos.')
        
    except mysql.connector.Error as err:
        if err.args[0] == 1062:
            logging.info(f'Valor {valor} de {codigo} ya registrado.')
            return True
        else:
            logging.error(f"Error insertar_valor_db(): {err}")
    finally:
        cursor.close()
        connection.close()

def obtener_parametros_db():
    try:
        connection = get_connection()

        if connection:
            logging.info('Conexión correcta a base de datos.')
            cursor = connection.cursor()
            query_api = "SELECT valores FROM configuracion WHERE tipo = 'API Banco Central' AND estado = 1;"
            cursor.execute(query_api)
            resultado_api = cursor.fetchall()
            usuario_api = eval(resultado_api[0][0])
            clave_api = usuario_api['clave']
            intentos =  usuario_api['intentos']      
            usuario_api = usuario_api['usuario']
            query_correo = "SELECT valores FROM configuracion WHERE tipo = 'correo' AND estado = 1;"
            cursor.execute(query_correo)
            resultado_correo = cursor.fetchall()
            resultado_correo = eval(resultado_correo[0][0])
            remitente = resultado_correo['remitente']
            clave_correo = resultado_correo['clave_correo']
            destinatarios =  resultado_correo['destinatarios']
            query_indicadores = "SELECT codigo, serie_codigo, periodicidad FROM indicador_economico WHERE estado_indicador = 1;"
            cursor.execute(query_indicadores)
            indicadores_a_buscar = cursor.fetchall()
            return usuario_api,clave_api,intentos,indicadores_a_buscar,remitente,clave_correo,destinatarios
        else:
            logging.info('No se pudo conectar a la base de datos.')
    except mysql.connector.Error as err:
        logging.error(f"Error obtener_parametros_db(): {err}")
    finally:
        cursor.close()
        connection.close()

def api_banco_central(usuario_api,clave_api,fecha_inicio,fecha_fin,serie):
    url = f"https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user={usuario_api}&pass={clave_api}&firstdate={fecha_inicio}&lastdate={fecha_fin}&timeseries={serie}&function=GetSeries"
    try:
        response = requests.get(url)
        if response.status_code == 200:
            json_api = json.loads(response.text)
            logging.debug(f'{json_api}')
            logging.info(f'Consulta REST a Banco Central devuelve un código: {str(response.status_code)}')
            return json_api
        else:            
            logging.info(f'Consulta REST a Banco Central devuelve un código: {str(response.status_code)}')
    except Exception as err:
        logging.error(f"Error api_banco_central(): {err}")

def dia_habil():
    try:
        hoy = datetime.now()
        #hoy = datetime.strptime("2024-10-31", "%Y-%m-%d").date()
        if hoy.weekday() == 5 or hoy.weekday() == 6:
            return False
        else:
            year = hoy.strftime("%Y")
            mes = hoy.strftime("%m")
            dia = hoy.strftime("%d")
            url_feriados = f'https://apis.digital.gob.cl/fl/feriados/{year}/{mes}/{dia}'
            headers_ = {"User-Agent" : "Mozilla/132.0 (X11; Linux x86_64)"}
            response = requests.get(url_feriados, headers = headers_)
            if response.status_code == 200:
                logging.info(f'Consulta REST API Feriados devuelve un código: {str(response.status_code)}')
                json_api = json.loads(response.text)
                if str(type(json_api)) == '''<class 'dict'>''' and json_api['error']:
                    logging.info(f'Consulta REST API Feriados: {str(json_api)}')
                    return True
                else:
                    logging.info(f'Consulta REST API Feriados: {str(json_api)}')
                    return False            
            else:            
                logging.info(f'Consulta REST API Feriados devuelve un código: {str(response.status_code)}')
    except Exception as err:
        logging.error(f"Error dia_habil(): {err}")


if __name__ == "__main__":
    logging.debug(f'--------------------- Inicio ---------------------')
    try: 
        error_ejecucion = set()
        usuario_api,clave_api,intentos,indicadores_a_buscar,remitente,clave_correo,destinatarios = obtener_parametros_db()
        if usuario_api and clave_api and indicadores_a_buscar:
            intentos = int(intentos)        
            for item in indicadores_a_buscar:
                contador_intentos = 1
                while contador_intentos <= intentos:
                    codigo = item[0]
                    serie = item[1]
                    periodicidad = item[2]
                    periodicidad = ast.literal_eval(periodicidad)
                    if periodicidad['diaria']:
                        es_dia_habil = dia_habil()
                        fecha_inicio = datetime.now().strftime("%Y-%m-%d")
                        fecha_fin = datetime.now().strftime("%Y-%m-%d")
                        if es_dia_habil is not None and es_dia_habil is True:
                            json_indicador = api_banco_central(usuario_api,clave_api,fecha_inicio,fecha_fin,serie)
                            if json_indicador['Descripcion'] == "Success":
                                valor_indicador = json_indicador['Series']['Obs'][0]['value']
                                res_valor = insertar_valor_db(fecha_inicio,valor_indicador,codigo)
                                if res_valor:
                                    logging.info(f"Valor {valor_indicador} de {codigo} guardado en base de datos")
                                    contador_intentos = intentos + 1
                                else:
                                    logging.info(f"Error en guardar valor {valor_indicador} de {codigo}")
                                    error_ejecucion.add(False)
                            else:
                                logging.info(f'Valor incorrecto en clave Descripcion en json de consulta a API para {codigo}.')
                                error_ejecucion.add(False)
                        elif es_dia_habil is not None and es_dia_habil is False:
                            valor_dia_anterior = obtener_valor_dia_anterior_db(codigo)
                            if valor_dia_anterior is not None:
                                valor_dia_anterior = valor_dia_anterior[0]
                                res_valor = insertar_valor_db(fecha_inicio,valor_dia_anterior,codigo)
                                if res_valor:
                                    logging.info(f"Valor {valor_dia_anterior} de {codigo} guardado en base de datos")
                                    contador_intentos = intentos + 1
                                else:
                                    logging.info(f"Error en guardar valor {valor_dia_anterior} de {codigo}")
                                    error_ejecucion.add(False)
                                
                            else:
                                logging.info(f'Error en obtener valor del día anterior de {codigo}')
                        else:
                            logging.info(f'Error en consulta de si hoy es un día hábil')
                            error_ejecucion.add(False)
                    else:
                        logging.debug(f'{codigo}, {serie}, {periodicidad}')
                        rango = periodicidad['rango']
                        inicio = rango['inicio']
                        termino = rango['termino']
                        fecha_inicio, fecha_fin = obtener_rango_fechas(inicio,termino)
                        if fecha_inicio is not None and fecha_fin is not None:
                            json_indicador = api_banco_central(usuario_api,clave_api,fecha_inicio,fecha_fin,serie)
                            if json_indicador['Descripcion'] == "Success":
                                lista_valores = json_indicador['Series']['Obs']
                                if len(lista_valores) > 0:
                                    inicio_fecha = datetime.strptime(fecha_inicio, '%Y-%m-%d')
                                    termino_fecha = datetime.strptime(fecha_fin, '%Y-%m-%d')
                                    contador = 0
                                    while inicio_fecha <= termino_fecha:
                                        string_dia = inicio_fecha.strftime("%Y-%m-%d")
                                        valor_api = lista_valores[contador]['value']
                                        res_valor = insertar_valor_db(string_dia,valor_api,codigo)
                                        if res_valor:
                                            logging.info(f"Valor {valor_api} de {codigo} guardado en base de datos")
                                        else:
                                            logging.info(f"Error en guardar valor {valor_api} de {codigo}")
                                            error_ejecucion.add(False)
                                        inicio_fecha += timedelta(days=1)
                                        if len(lista_valores) != 1:
                                            contador += 1
                                    contador_intentos = intentos + 1
                                else:
                                    logging.info(f'Error, no existen valores para {codigo}.')
                                    error_ejecucion.add(False)
                            else:
                                logging.info(f'Error, valor incorrecto en clave Descripcion en json de consulta a API para {codigo}.')
                                error_ejecucion.add(False)
                        else:
                            logging.info(f'Fuera de rango de fechas de indicador {codigo}')
                            contador_intentos = intentos + 1     
                    contador_intentos += 1
        else:
            logging.info(f'Error en consulta de valores de parámetros')
            error_ejecucion.add(False)
        if False in error_ejecucion:
            asunto_correo = "Error - Indicadores económicos"
            cuerpo_correo = "Proceso que obtiene valores de indicadores económicos no pudo finalizar correctamente, favor revisar."
        else:
            asunto_correo = "OK - Indicadores económicos"
            cuerpo_correo = "Proceso que obtiene valores de indicadores económicos finalizó correctamente."
        correo_enviado = envio_correo(remitente,clave_correo,destinatarios,asunto_correo,cuerpo_correo)
        logging.info(f'Envío de correo: {correo_enviado}')
    except Exception as err:
        logging.error(f"Error __main__: {err}")