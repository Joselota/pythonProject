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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_pago_expo")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_movimmat")

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_facturaexpoaportes
    kupay_cursor.execute('select Num_PE, NumeroFactEx, Monto, fecha_pe, Documento, MontoPesos from pago_expo')
    print("(57) tabla pago_expo")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Num_PE, NumeroFactEx, Monto, fecha_pe, Documento, MontoPesos in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_pago_expo (Num_PE, NumeroFactEx, Monto, fecha_pe, Documento, MontoPesos) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (Num_PE, NumeroFactEx, Monto, fecha_pe, Documento, MontoPesos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla pago_expo: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'pago_expo', 'bdg_pago_expo', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_facturaexpoaportes
    kupay_cursor.execute('SELECT CodMov, Fecha, NumDoc, TipDoc, CodMat, Ingreso, Egreso, NFicha, Documento, Saldo, ValorPMP, Hora, SaldoPesos, CodBod, CostoUnit, MontoIngreso, MontoEgreso, NumODC, FechaReal, Centralizada, CabOpeNumero FROM movimmat;')
    print("(58) tabla movimmat")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for CodMov, Fecha, NumDoc, TipDoc, CodMat, Ingreso, Egreso, NFicha, Documento, Saldo, ValorPMP, Hora, SaldoPesos, CodBod, CostoUnit, MontoIngreso, MontoEgreso, NumODC, FechaReal, Centralizada, CabOpeNumero in kupay_cursor.fetchall():
        i = i + 1
        if str(ValorPMP) == 'inf':
            ValorPMP = 0
        if str(SaldoPesos) == 'inf':
            SaldoPesos = 0
        sql = "INSERT INTO " + EsquemaBD + ".bdg_movimmat (CodMov, Fecha, NumDoc, TipDoc, CodMat, Ingreso, Egreso, NFicha, Documento, Saldo, ValorPMP, Hora, SaldoPesos, CodBod, CostoUnit, MontoIngreso, MontoEgreso, NumODC, FechaReal, Centralizada, CabOpeNumero) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodMov, Fecha, NumDoc, TipDoc, CodMat, Ingreso, Egreso, NFicha, Documento, Saldo, ValorPMP, Hora, SaldoPesos, CodBod, CostoUnit, MontoIngreso, MontoEgreso, NumODC, FechaReal, Centralizada, CabOpeNumero)
        print(sql, val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_movimmat: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'movimmat', 'bdg_movimmat', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")