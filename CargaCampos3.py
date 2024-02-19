import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

# VariablesGlobales
EsquemaBD = "stagecampos"
SistemaOrigen = "Campos"
fechacarga = datetime.datetime.now()

# Generando identificador para proceso de cuadratura
dia = str(100+int(format(fechacarga.day)))
mes = str(100+int(format(fechacarga.month)))
agno = format(fechacarga.year)
Identificador = str(agno) + str(mes[1:]) + str(dia[1:])

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".costolab")
print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
Campos = pyodbc.connect('DSN=CamposV3')
campos_cursor = Campos.cursor()

#SELECT CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad FROM costolab;
# TABLA costolab
i = 0
campos_cursor.execute('SELECT CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad FROM costolab')
registrosorigen = campos_cursor.rowcount
print("(4) tabla costolab")
print(registrosorigen)
for CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad in campos_cursor.fetchall():
    i = i + 1
    if str(Costo) == 'inf':
        Costo = 0
    print(CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad)
    sql = "INSERT INTO " + EsquemaBD + ".costolab(CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad) " \
                                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla costolab: ", i)

# Cierre de cursores y bases de datos
campos_cursor.close()
Campos.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")
