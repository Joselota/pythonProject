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
kupay_cursor.execute('select count(*) as cant from submarca')

if kupay_cursor.rowcount <= 0:
    print("NO HAY REGISTROS")
    sys.exit(-1)
else:
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_origen")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)


  # TABLA deta_embfactn 24
    i = 0
    kupay_cursor.execute('select Num_ODB, Codvino, Codigo_vino, Cuba, TipoVino, Cuba_ant, Cuba_PP, Labor, Qty, '
                         'Stock_real, Cuba_Mezcla, NomVari, Ltrs_Mezcla, Cod_Mezcla, Cod_IntMez, Grupo_Bar, '
                         'NBarr, Cap_Cuba, CubaMez_AP, CodVinDesp, CubaBarr, NumItem, Cosecha, AptiOr, Enuso, '
                         'KilosAP, KilosPP, KilosGuiasAP, LtsGota, LtsPrensa from origen')
    registrosorigen = kupay_cursor.rowcount
    print("(1) tabla origen")
    for Num_ODB, Codvino, Codigo_vino, Cuba, TipoVino, Cuba_ant, Cuba_PP, Labor, Qty, Stock_real, Cuba_Mezcla, \
        NomVari, Ltrs_Mezcla, Cod_Mezcla, Cod_IntMez, Grupo_Bar, NBarr, Cap_Cuba, CubaMez_AP, CodVinDesp, \
        CubaBarr, NumItem, Cosecha, AptiOr, Enuso, KilosAP, KilosPP, KilosGuiasAP, LtsGota, LtsPrensa in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_origen (Num_ODB, Codvino, Codigo_vino, Cuba, TipoVino, Cuba_ant, " \
                                           "Cuba_PP, Labor, Qty, Stock_real, Cuba_Mezcla, NomVari, Ltrs_Mezcla, " \
                                           "Cod_Mezcla, Cod_IntMez, Grupo_Bar, NBarr, Cap_Cuba, CubaMez_AP, " \
                                           "CodVinDesp, CubaBarr, NumItem, Cosecha, AptiOr, Enuso, KilosAP, KilosPP, " \
                                           "KilosGuiasAP, LtsGota, LtsPrensa) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
        val = (Num_ODB, Codvino, Codigo_vino, Cuba, TipoVino, Cuba_ant, Cuba_PP, Labor, Qty, Stock_real, Cuba_Mezcla,
               NomVari, Ltrs_Mezcla, Cod_Mezcla, Cod_IntMez, Grupo_Bar, NBarr, Cap_Cuba, CubaMez_AP, CodVinDesp,
               CubaBarr, NumItem, Cosecha, AptiOr, Enuso, KilosAP, KilosPP, KilosGuiasAP, LtsGota, LtsPrensa)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla deta_embfactn: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura(id,SistemaOrigen,TablaOrigen,TablaDestino,NroRegistroOrigen," \
                                       "NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'origen', 'bdg_origen', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
