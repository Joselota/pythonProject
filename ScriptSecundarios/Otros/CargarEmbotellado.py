import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

# Generando identificador para proceso de cuadratura
dia = str(100+int(format(fechacarga.day)))
mes = str(100+int(format(fechacarga.month)))
agno = format(fechacarga.year)
Identificador = str(agno) + str(mes[1:]) + str(dia[1:])


# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

print("Consultando disponibilidad de base de datos Kupay")
kupay_cursor.execute("select count(*) as cant from submarca")
print(kupay_cursor.rowcount)
if kupay_cursor.rowcount <= 0:
    print("NO HAY REGISTROS")
    sys.exit(-1)
else:
    print("OK")
    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso" + localtime)
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_embotellado")
    print("Fin del proceso de truncado de tablas")
   # TABLA facturas 19
    i = 0
    kupay_cursor.execute('SELECT Num_ODB, CodMat, Cant, 0 as cant_Real ,CodFam, Merma, NItem, '
                         'Tipo_Vino, Cosecha, Litros, Familia, Existencia, Tipo, LoteODB FROM embotellado')
    registrosorigen = kupay_cursor.rowcount
    print("(19) tabla embotellado")
    for Num_ODB, CodMat, Cant, cant_Real, CodFam, Merma, NItem, Tipo_Vino, Cosecha, Litros, Familia, Existencia, Tipo, LoteODB in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_embotellado (Num_ODB, CodMat, Cant, cant_Real, CodFam, Merma, " \
                                           "NItem, Tipo_Vino, Cosecha, Litros, Familia, Existencia, Tipo, " \
                                           "LoteODB) VALUES (%s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s)"
        val = (Num_ODB, CodMat, Cant, cant_Real, CodFam, Merma, NItem, Tipo_Vino, Cosecha, Litros, Familia, Existencia, Tipo, LoteODB)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla embotellado: ", i)

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")