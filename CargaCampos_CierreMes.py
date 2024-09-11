import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

# VariablesGlobales
EsquemaBD = "stagecampos"
SistemaOrigen = "Campos"
fechacarga = datetime.now()

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

# bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".costolabores")


print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
Campos = pyodbc.connect('DSN=CamposV3')
campos_cursor = Campos.cursor()

# TABLA costolab
i = 0
campos_cursor.execute('SELECT CodLabor, CostoTotal, JornHombre, CodCuartel, CostoTotalCtta, TipoCostoLabor, '
                      'Inicio, Termino, Dias, Costo, Trato, HorasExtras, QHE, Bono, Liquida, Aportes, '
                      'Finiquitos FROM costolabores')
registrosorigen = campos_cursor.rowcount
print("(20) tabla costolabores")
print(registrosorigen)
for CodLabor, CostoTotal, JornHombre, CodCuartel, CostoTotalCtta, TipoCostoLabor, Inicio, Termino, Dias, Costo, Trato, HorasExtras, QHE, Bono, Liquida, Aportes, Finiquitos in campos_cursor.fetchall():
    i = i + 1
    print(CodLabor, CostoTotal, JornHombre, CodCuartel, CostoTotalCtta, TipoCostoLabor, Inicio, Termino, Dias, Costo, Trato, HorasExtras, QHE, Bono, Liquida, Aportes, Finiquitos)
    sql = "INSERT INTO " + EsquemaBD + ".costolabores(CodLabor, CostoTotal, JornHombre, CodCuartel, " \
                                       "CostoTotalCtta, TipoCostoLabor, Inicio, Termino, Dias, Costo, " \
                                       "Trato, HorasExtras, QHE, Bono, Liquida, Aportes, Finiquitos) " \
                                        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodLabor, CostoTotal, JornHombre, CodCuartel, CostoTotalCtta, TipoCostoLabor, Inicio, Termino, Dias, Costo, Trato, HorasExtras, QHE, Bono, Liquida, Aportes, Finiquitos)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla costolabores: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'costolabores', 'costolabores', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()


# Cierre de cursores y bases de datos
campos_cursor.close()
Campos.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")