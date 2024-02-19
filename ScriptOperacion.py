import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD
from Tools.funciones import f_limpiar

# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

#Generando identificador para proceso de cuadratura
dia = str(100+int(format(fechacarga.day)))
mes = str(100+int(format(fechacarga.month)))
agno = format(fechacarga.year)
Identificador = str(agno) + str(mes[1:]) + str(dia[1:])
print("****")
# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

## Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

print("Consultando disponibilidad de base de datos Kupay")
kupay_cursor.execute('select count(*) as cant from submarca')
registrosorigen = kupay_cursor.rowcount
if registrosorigen == 0:
    print("Sin disponibilidad Kupay")
    exit
else:
    print("Se inicia proceso de carga")
    ## Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso: " + localtime)
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_operacion")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_proceso")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    ## TABLA proceso
    kupay_cursor.execute('select CodPro, Nombre, NSecu, Cod_Estado, Valor_Un, Pide_Ficha, Ponder, Nacional, Export, CuentaCargo, CodOper, CodMerma, CodEstOrigen, Reproceso, CodOpCostoStd, CostoLitroEmbo from proceso')
    print("(63) tabla bdg_proceso")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for CodPro, Nombre, NSecu, Cod_Estado, Valor_Un, Pide_Ficha, Ponder, Nacional, Export, CuentaCargo, CodOper, CodMerma, CodEstOrigen, Reproceso, CodOpCostoStd, CostoLitroEmbo in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO bdg_proceso (CodPro, Nombre, NSecu, Cod_Estado, Valor_Un, Pide_Ficha, Ponder, Nacional, Export, CuentaCargo, CodOper, CodMerma, CodEstOrigen, Reproceso, CodOpCostoStd, CostoLitroEmbo) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodPro, Nombre, NSecu, Cod_Estado, Valor_Un, Pide_Ficha, Ponder, Nacional, Export, CuentaCargo, CodOper, CodMerma, CodEstOrigen, Reproceso, CodOpCostoStd, CostoLitroEmbo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_proceso: ", i)

    #Proceso cuadratura de carga
    sql = "INSERT INTO proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'proceso', 'bdg_proceso', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    ## TABLA bdg_operacion
    kupay_cursor.execute('select CodOper, Operacion, CuentaCargo, TipoOper, NoVigente, FechaEstado, UltUsuarioMod from operacion')
    print("(64) tabla bdg_operacion")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for CodOper, Operacion, CuentaCargo, TipoOper, NoVigente, FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO bdg_operacion (CodOper, Operacion, CuentaCargo, TipoOper, NoVigente, FechaEstado, UltUsuarioMod) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (CodOper, Operacion, CuentaCargo, TipoOper, NoVigente, FechaEstado, UltUsuarioMod)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_proceso: ", i)

    #Proceso cuadratura de carga
    sql = "INSERT INTO proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'operacion', 'bdg_operacion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    ## Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")