import requests
import pymysql
import time
import pandas as pd
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, BC_USUARIO, BC_TOKEN, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

def main():
    # Inicializar variables
    # VariablesGlobales
    lista = []
    limite = 50
    BC_firstdate = "2012-01-01"
    BC_lastdate = ""
    EsquemaBD = "bcentral"

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    print("Truncando tablas")
    bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".bc_detalle")
    print("Fin truncando tablas")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    # Leer tabla de documentos
    sql = "SELECT distinct seriesId from bcentral.bc_serieeconomica"
    bdg_cursor.execute(sql)
    myresult = bdg_cursor.fetchall()
    print('Total Row(s):', bdg_cursor.rowcount)
    for row in myresult:
        url = "https://si3.bcentral.cl/SieteRestWS/SieteRestWS.ashx?user="+str(BC_USUARIO) + \
              "&pass="+str(BC_TOKEN)+"&firstdate="+str(BC_firstdate)+"&timeseries="+str(row[0])+"&function=GetSeries"
        print(url)
        response = requests.get(url)
        lista = response.json()
        lista = lista["Series"]["Obs"]

        for indice in lista:
            sql = "INSERT INTO "+EsquemaBD+".bc_detalle (seriesId, indexDateString, value, statusCode) " \
                                           "VALUES (%s,%s,%s,%s) "
            val = (str(row[0]), indice["indexDateString"], indice["value"], indice["statusCode"])
            bdg_cursor.execute(sql, val)
            bdg.commit()

    # Truncacion de fecha carga
    bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".FechaCargaInformacion")

    # Registro de fecha cargada en base de datos
    Proceso = 'P03'
    Descripcion = 'Carga API BCENTRAL'
    fecha = time.localtime(time.time())
    sql = "INSERT INTO "+EsquemaBD+".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
    val = (Proceso, Descripcion, fecha)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    bdg.close()
    bdg_cursor.close()


if __name__ == "__main__":
    main()