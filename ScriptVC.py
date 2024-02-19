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
kupay = pyodbc.connect('DSN=kupayC', readonly=True)
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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vale_consumo")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_vc")

    ######################
    # Cargar tablas      #
    ######################
    # TABLA vale_consumo #
    ######################
    QuerySQL="SELECT Numero, Tipo, Fecha, Bod_Out, CodBod, SUBSTRING(Glosa_Vale,1,500) as Glosa_Vale, Enc, MS_INS, Realiza, Ccosto, " \
             "CodOper, DeProduccion, Ficha, FechaIngreso, Centralizada, CabOpeNumero FROM vale_consumo"
    kupay_cursor.execute(QuerySQL)
    print("(60) tabla bdg_vale_consumo")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    print(registrosorigen)
    print("hola i")
    #records = kupay_cursor.fetchone()
    for Numero, Tipo, Fecha, Bod_Out, CodBod, Glosa_Vale, Enc, MS_INS, Realiza, Ccosto, CodOper, DeProduccion, Ficha, FechaIngreso, Centralizada, CabOpeNumero in kupay_cursor:
        i = i + 1
        print(Numero, Tipo, Fecha, Bod_Out, CodBod, Glosa_Vale, Enc, MS_INS, Realiza, Ccosto, CodOper, DeProduccion, Ficha, FechaIngreso, Centralizada, CabOpeNumero)
        sql = "INSERT INTO " + EsquemaBD + ".bdg_vale_consumo (Numero, Tipo, Fecha, Bod_Out, CodBod, Glosa_Vale, Enc, MS_INS, Realiza, Ccosto, CodOper, DeProduccion, Ficha, FechaIngreso, Centralizada, CabOpeNumero) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Numero, Tipo, Fecha, Bod_Out, CodBod, Glosa_Vale, Enc, MS_INS, Realiza, Ccosto, CodOper, DeProduccion, Ficha, FechaIngreso, Centralizada, CabOpeNumero)
        #print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("hola f")
    print("Cantidad de registros en la tabla bdg_vale_consumo: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'vale_consumo', 'bdg_vale_consumo', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    ######################
    # TABLA bdg_detalle_vc #
    ######################
    QuerySQL = "SELECT ID_VC, Cod_Ins, CodMat, Cantidad, DetBod, Valor, Descripcion, Codigo, " \
               "CtaAbono, CCostoAbono, Existencia, QSolicitada, MontoTotal, Trasp, CodMatDestino, " \
               "QMargen, QTotal, Importado FROM detalle_vc"
    kupay_cursor.execute(QuerySQL)
    print("(61) tabla bdg_detalle_vc")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    print(registrosorigen)
    print("hola i")
    # records = kupay_cursor.fetchone()
    for ID_VC, Cod_Ins, CodMat, Cantidad, DetBod, Valor, Descripcion, Codigo, CtaAbono, CCostoAbono, Existencia, QSolicitada, MontoTotal, Trasp, CodMatDestino, QMargen, QTotal, Importado in kupay_cursor:
        i = i + 1
        if str(Valor) == 'inf':
            Valor = 0
        if str(Valor) == '-inf':
            Valor = 0
        print(ID_VC, Cod_Ins, CodMat, Cantidad, DetBod, Valor, Descripcion, Codigo, CtaAbono, CCostoAbono, Existencia, QSolicitada, MontoTotal, Trasp, CodMatDestino, QMargen, QTotal, Importado)
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_vc (ID_VC, Cod_Ins, CodMat, Cantidad, DetBod, Valor, Descripcion, Codigo, CtaAbono, CCostoAbono, Existencia, QSolicitada, MontoTotal, Trasp, CodMatDestino, QMargen, QTotal, Importado) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (ID_VC, Cod_Ins, CodMat, Cantidad, DetBod, Valor, Descripcion, Codigo, CtaAbono, CCostoAbono, Existencia, QSolicitada, MontoTotal, Trasp, CodMatDestino, QMargen, QTotal, Importado)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("hola f")
    print("Cantidad de registros en la tabla bdg_detalle_vc: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalle_vc', 'bdg_detalle_vc', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")