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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_kardex_granel")

    ######################
    # Cargar tablas
    ######################
    # TABLA bdg_kardex_granel
    kupay_cursor.execute('SELECT Fecha, Documento, TipoVino, Cosecha, Qty, Ingreso, Egreso, Saldo, Tipo_Doc, CostoMedio, Operacion, VinoCos, SaldoPesos, Centralizada, CabOpeNumero, MontoIngreso, MontoEgreso, Tmov, MovODB, Hora, Id_costo, FechaReal FROM kardex_granel')
    print("(59) tabla bdg_kardex_granel")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Fecha, Documento, TipoVino, Cosecha, Qty, Ingreso, Egreso, Saldo, Tipo_Doc, CostoMedio, Operacion, VinoCos, SaldoPesos, Centralizada, CabOpeNumero, MontoIngreso, MontoEgreso, Tmov, MovODB, Hora, Id_costo, FechaReal in kupay_cursor.fetchall():
        i = i + 1
        if str(CostoMedio) == 'inf':
            CostoMedio = 0
        if str(CostoMedio) == '-inf':
            CostoMedio = 0
        sql = "INSERT INTO " + EsquemaBD + ".bdg_kardex_granel (Fecha, Documento, TipoVino, Cosecha, Qty, Ingreso, Egreso, Saldo, Tipo_Doc, CostoMedio, Operacion, VinoCos, SaldoPesos, Centralizada, CabOpeNumero, MontoIngreso, MontoEgreso, Tmov, MovODB, Hora, Id_costo, FechaReal) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Fecha, Documento, TipoVino, Cosecha, Qty, Ingreso, Egreso, Saldo, Tipo_Doc, CostoMedio, Operacion, VinoCos, SaldoPesos, Centralizada, CabOpeNumero, MontoIngreso, MontoEgreso, Tmov, MovODB, Hora, Id_costo, FechaReal)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla kardex_granel: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'kardex_granel', 'bdg_kardex_granel', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")