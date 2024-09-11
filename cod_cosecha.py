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
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_cod_cosecha")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")


    # TABLA costovino
    i = 0
    kupay_cursor.execute('SELECT CodVino, ID_VinCos, Prom_Brix, Prom_Dens, Prom_Temp, Cuba, Fecha, LitrosGuias, '
                         'KilosCuba, TipoMol, Prend, LitrosCubaDes, Densidad, Brix, PH, SO2L, SO2T, AnalisisSC, '
                         'NTU, Nitrogeno, AcdTotal, KilosGuias, TipJ, Descube, NCierre, Cubada, CodVinoJugo, '
                         'Eliminar FROM cod_cosecha')
    registrosorigen = kupay_cursor.rowcount
    print("tabla cod_cosecha")
    for CodVino, ID_VinCos, Prom_Brix, Prom_Dens, Prom_Temp, Cuba, Fecha, LitrosGuias, KilosCuba, TipoMol, Prend, LitrosCubaDes, Densidad, Brix, PH, SO2L, SO2T, AnalisisSC, NTU, Nitrogeno, AcdTotal, KilosGuias, TipJ, Descube, NCierre, Cubada, CodVinoJugo, Eliminar in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_cod_cosecha (CodVino, ID_VinCos, Prom_Brix, Prom_Dens, Prom_Temp, " \
                                           "Cuba, Fecha, LitrosGuias, KilosCuba, TipoMol, Prend, LitrosCubaDes, " \
                                           "Densidad, Brix, PH, SO2L, SO2T, AnalisisSC, NTU, Nitrogeno, AcdTotal, " \
                                           "KilosGuias, TipJ, Descube, NCierre, Cubada, CodVinoJugo, Eliminar)" \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodVino, ID_VinCos, Prom_Brix, Prom_Dens, Prom_Temp, Cuba, Fecha, LitrosGuias, KilosCuba, TipoMol, Prend, LitrosCubaDes, Densidad, Brix, PH, SO2L, SO2T, AnalisisSC, NTU, Nitrogeno, AcdTotal, KilosGuias, TipJ, Descube, NCierre, Cubada, CodVinoJugo, Eliminar)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_enc_rec_uva: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cod_cosecha', 'bdg_cod_cosecha', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")