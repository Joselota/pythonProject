import pyodbc
import pymysql
import time
import datetime

# VariablesGlobales
IPServidor = "127.0.0.1"
UsuarioBD = "operador"
PasswordBD = "Viu2022"
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

#Generando identificador para proceso de cuadratura
dia = str(100+int(format(fechacarga.day)))
mes = str(100+int(format(fechacarga.month)))
agno = format(fechacarga.year)
Identificador = str(agno) + str(mes[1:]) + str(dia[1:])

## Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_condicionpago")
print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

## Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

## Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

## TABLA bdg_condicionpago 37
print("(37) tabla bdg_condicionpago")
kupay_cursor.execute("SELECT codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte  FROM condicionpago")
registrosorigen = kupay_cursor.rowcount
i = 0
for codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte in kupay_cursor.fetchall():
    i = i + 1
    #print(codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte)
    sql = "INSERT INTO bdg_condicionpago(codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte) VALUES (%s, %s, %s, %s, %s, %s)"
    val = (codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla bdg_condicionpago: ", i)
#Proceso cuadratura de carga
sql = "INSERT INTO proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'condicionpago', 'bdg_condicionpago', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# Muestra fecha y hora actual al finalizar el proceso
localtime2 = time.asctime(time.localtime(time.time()))
print("Fecha y hora de finalizacion del proceso")
print(localtime2)

## Cierre de cursores y bases de datos
kupay_cursor.close()
kupay.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")