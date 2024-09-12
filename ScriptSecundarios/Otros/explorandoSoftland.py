import pymysql
import time
import pyodbc as pyodbc
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from DatosConexion.VG import DRIVER, SERVER, DATABASE, UID, PWD

# VariablesGlobales
EsquemaBD = "stagesoftland"
periodo = 2023

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Base de datos Softland (Desde donde se leen los datos)
stringConn: str = (
        'Driver={' + DRIVER + '};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + UID + ';PWD=' + PWD + ';')
#print(stringConn)
i = 0
try:
    conn = pyodbc.connect(stringConn)
    cursor = conn.cursor()

    # cargar tabla cwtdetg
    sql = "SELECT CodiCC, DescCC, NivelCc FROM softland.CWTCCOS where len(DescCC)>0 AND DescCC<>'No Utilizar' "
    cursor.execute(sql)
    i = 0
    for CodiCC, DescCC, NivelCc in cursor.fetchall():
        i = i + 1
        sql = "insert into stagesoftland.CWTCCOS (CodiCC, DescCC, NivelCc)" \
              " VALUES (%s, %s, %s )"
        val = (CodiCC, DescCC, NivelCc)
        bdg_cursor.execute(sql, val)
        bdg.commit()
        print(CodiCC, DescCC, NivelCc)
    print("Total registros cargados a DL :" + str(i))

    # Cerrar la conexión a softland
    cursor.close()
    conn.close()


except pyodbc.DataError as err:
    print("An exception occurred")
    print(err)

# Cerrar la conexión del datalake
bdg_cursor.close()
bdg.close()