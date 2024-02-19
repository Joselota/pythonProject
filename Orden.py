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
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_origen_lote")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalleembotellado")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_orden_bodega")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    # TABLA bdg_detalleembotellado
    print("(59) tabla bdg_orden_bodega")
    kupay_cursor.execute("select Num_ODB, Fecha_OB,Enc_OB, Realiza,Observac,Realiza2, Labor,Done, Lote, Nula, FMezcla, "
                         "Tipo_Trasiego, Nombre, NGuia, CodLinea, CodProducto, FDeshecha, OTPedido, CodOper, CodBodMS, "
                         "NumIngreso, CodBodPT, NumFicha, UnidEmbalaje, CodEstado, TvinoMezcla, Instrucc, CodBodINS, "
                         "Centralizada, CabOpeNumero from orden_bodega")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Num_ODB, Fecha_OB, Enc_OB, Realiza, Observac, Realiza2, Labor, Done, Lote, Nula, FMezcla, Tipo_Trasiego, \
        Nombre, NGuia, CodLinea, CodProducto, FDeshecha, OTPedido, CodOper, CodBodMS, NumIngreso, CodBodPT, \
        NumFicha, UnidEmbalaje, CodEstado, TvinoMezcla, Instrucc, CodBodINS, Centralizada, \
         CabOpeNumero in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_orden_bodega (Num_ODB, Fecha_OB, Enc_OB, Realiza, Observac, Realiza2,"\
                                           "Labor, Done, Lote, Nula, FMezcla, Tipo_Trasiego, Nombre, NGuia, CodLinea,"\
                                           "CodProducto, FDeshecha, OTPedido, CodOper, CodBodMS, NumIngreso, CodBodPT,"\
                                           "NumFicha, UnidEmbalaje, " \
                                           "CodEstado, TvinoMezcla, Instrucc, CodBodINS, Centralizada, CabOpeNumero) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Num_ODB, Fecha_OB, Enc_OB, Realiza, Observac, Realiza2, Labor, Done, Lote, Nula, FMezcla, Tipo_Trasiego,
               Nombre, NGuia, CodLinea, CodProducto, FDeshecha, OTPedido, CodOper, CodBodMS, NumIngreso, CodBodPT,
               NumFicha, UnidEmbalaje, CodEstado, TvinoMezcla, Instrucc, CodBodINS, Centralizada, CabOpeNumero)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_orden_bodega: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'orden_bodega', 'bdg_orden_bodega', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_origen_lote
    print("(57) tabla bdg_origen_lote")
    kupay_cursor.execute("SELECT Lote, Lote_or, QtyLote, MermaLote, CodVino_Or from origen_lote")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Lote, Lote_or, QtyLote, MermaLote, CodVino_Or in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_origen_lote(Lote, Lote_or, QtyLote, MermaLote, CodVino_Or) " \
                                           "VALUES (%s, %s, %s, %s, %s )"
        val = (Lote, Lote_or, QtyLote, MermaLote, CodVino_Or)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_origen_lote: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'origen_lote', 'bdg_origen_lote', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_detalleembotellado
    print("(58) tabla bdg_detalleembotellado")
    kupay_cursor.execute("select Num_ODB, TotalBotUtiliz, TotBotRechazo, TotBotQuebrdLlenas, TotBotQuebVacias, "
                         "TotCorchosUtiliz, TotMermaCorcho, DevLitros, ProduccTotalBot, MuestrasLabBot, "
                         "TestigosCCalidBot, ProduccCjs, ProduccLts, CodBot, NomBot, CodCorcho, NomCorcho, "
                         "TipoVIno, Cosecha, ExistBotella, ExistCorcho, CapacBot, TotalSolic, HI, HT, TotalHoras, "
                         "TPerdido, VelTRabajo, TOperacReal, PorcPerfTurno, TotalHH, MermaFiltro, LIneaProduccion, "
                         "CostoBot, CostoCorcho, NumPersonas from detalleembotellado")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Num_ODB, TotalBotUtiliz, TotBotRechazo, TotBotQuebrdLlenas, TotBotQuebVacias, TotCorchosUtiliz, TotMermaCorcho,\
        DevLitros, ProduccTotalBot, MuestrasLabBot, TestigosCCalidBot, ProduccCjs, ProduccLts, CodBot, NomBot, \
        CodCorcho, NomCorcho, TipoVIno, Cosecha, ExistBotella, ExistCorcho, CapacBot, TotalSolic, HI, HT, TotalHoras, \
        TPerdido, VelTRabajo, TOperacReal, PorcPerfTurno, TotalHH, MermaFiltro, LIneaProduccion, CostoBot, \
         CostoCorcho, NumPersonas in kupay_cursor.fetchall():
        i = i + 1
        if str(ProduccCjs) == 'inf':
            ProduccCjs = 0
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detalleembotellado(Num_ODB, TotalBotUtiliz, TotBotRechazo, " \
                                           "TotBotQuebrdLlenas, TotBotQuebVacias, TotCorchosUtiliz, TotMermaCorcho, " \
                                           "DevLitros, ProduccTotalBot, MuestrasLabBot, TestigosCCalidBot,ProduccCjs," \
                                           "ProduccLts, CodBot, NomBot, CodCorcho, NomCorcho, TipoVIno, Cosecha, " \
                                           "ExistBotella, ExistCorcho, CapacBot, TotalSolic, HI, HT, TotalHoras, " \
                                           "TPerdido, VelTRabajo, TOperacReal, PorcPerfTurno, TotalHH, MermaFiltro, " \
                                           "LIneaProduccion, CostoBot, CostoCorcho, NumPersonas) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s)"
        val = (Num_ODB, TotalBotUtiliz, TotBotRechazo, TotBotQuebrdLlenas, TotBotQuebVacias, TotCorchosUtiliz,
               TotMermaCorcho, DevLitros, ProduccTotalBot, MuestrasLabBot, TestigosCCalidBot, ProduccCjs, ProduccLts,
               CodBot, NomBot, CodCorcho, NomCorcho, TipoVIno, Cosecha, ExistBotella, ExistCorcho, CapacBot,
               TotalSolic, HI, HT, TotalHoras, TPerdido, VelTRabajo, TOperacReal, PorcPerfTurno, TotalHH, MermaFiltro,
               LIneaProduccion, CostoBot, CostoCorcho, NumPersonas)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_detalleembotellado: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalleembotellado', 'bdg_detalleembotellado', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Muestra fecha y hora actual al finalizar el proceso
    localtime2 = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de finalizacion del proceso")
    print(localtime2)

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")