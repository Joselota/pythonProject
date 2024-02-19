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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_guia_despacho")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_apelacion")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_guia_despacho 56
    i = 0
    kupay_cursor.execute("select Numero_GD, Fecha_GD, CodCli_GD, NumPedido, NFicha_GD, TipFicha, Moneda, Cod_Banco, "
                         "Pto_Emb, Pto_Dest, Nave, Contenedor, Num_Sello, Pat_Camion, Chofer, Hora_Salida, Neto_GD, "
                         "Iva_GD, Ila_GD, Total_GD, DeTraslado, NumTraslado, Observacion, Impresa, Cod_Oper, "
                         "Nula, Factura, CtoVenta, TotalLitros, CodBod, FormaPagoGlosa, CodCtaCtble, CodigoEmp, "
                         "SucCodigo, CorrEmpresa, PesoNeto, PesoBruto, TotalCajas, TotalCajasLotes, FormaPagoCodigo, "
                         "Camion, Carro, Pat_Carro, Referencia, Referencia2, CodBodtraslado, Tipo_Doc, Mes, "
                         "ModificaStock, ParaNCredCpra, TieneNCredCpra, Centralizada, MotivoAnulacion, Semana, "
                         "Serie, Bruto, Tara, Densidad, LtsCamion, CodigoOC, AutorizadoPor, ClieBloqueado, "
                         "IndicadorMora, SaldoCredito, Estado, FechaAutoriza, ConCorrPreliminar, WS_NumCabID, "
                         "WS_FolioSII, CDITotal, CDIIVA, CDIILA, CodCC, CabOpeNumero FROM guia_despacho")
    registrosorigen = kupay_cursor.rowcount
    print("(56) tabla bdg_guia_despacho")
    for Numero_GD, Fecha_GD, CodCli_GD, NumPedido, NFicha_GD, TipFicha, Moneda, Cod_Banco, Pto_Emb, Pto_Dest, Nave, \
        Contenedor, Num_Sello, Pat_Camion, Chofer, Hora_Salida, Neto_GD, Iva_GD, Ila_GD, Total_GD, DeTraslado, \
        NumTraslado, Observacion, Impresa, Cod_Oper, Nula, Factura, CtoVenta, TotalLitros, CodBod, FormaPagoGlosa, \
        CodCtaCtble, CodigoEmp, SucCodigo, CorrEmpresa, PesoNeto, PesoBruto, TotalCajas, TotalCajasLotes, \
        FormaPagoCodigo, Camion, Carro, Pat_Carro, Referencia, Referencia2, CodBodtraslado, Tipo_Doc, Mes, \
        ModificaStock, ParaNCredCpra, TieneNCredCpra, Centralizada, MotivoAnulacion, Semana, Serie, Bruto, \
        Tara, Densidad, LtsCamion, CodigoOC, AutorizadoPor, ClieBloqueado, IndicadorMora, SaldoCredito, \
        Estado, FechaAutoriza, ConCorrPreliminar, WS_NumCabID, WS_FolioSII, CDITotal, CDIIVA, CDIILA, \
         CodCC, CabOpeNumero in kupay_cursor.fetchall():
       i = i+1
       sql = "INSERT INTO " + EsquemaBD + ".bdg_guia_despacho(Numero_GD, Fecha_GD, CodCli_GD, NumPedido," \
                                          "NFicha_GD, TipFicha, Moneda, Cod_Banco, Pto_Emb, Pto_Dest, Nave, " \
                                          "Contenedor, Num_Sello, Pat_Camion, Chofer, Hora_Salida, Neto_GD, " \
                                          "Iva_GD, Ila_GD, Total_GD, DeTraslado, NumTraslado, Observacion, " \
                                          "Impresa, Cod_Oper, Nula, Factura, CtoVenta, TotalLitros, CodBod, " \
                                          "FormaPagoGlosa, CodCtaCtble, CodigoEmp, SucCodigo, CorrEmpresa, " \
                                          "PesoNeto, PesoBruto, TotalCajas, TotalCajasLotes, FormaPagoCodigo, " \
                                          "Camion, Carro, Pat_Carro, Referencia, Referencia2, CodBodtraslado, " \
                                          "Tipo_Doc, Mes, ModificaStock, ParaNCredCpra, TieneNCredCpra, " \
                                          "Centralizada, MotivoAnulacion, Semana, Serie, Bruto, Tara, Densidad, " \
                                          "LtsCamion, CodigoOC, AutorizadoPor, ClieBloqueado, IndicadorMora," \
                                          " SaldoCredito, Estado, FechaAutoriza, ConCorrPreliminar, WS_NumCabID, " \
                                          "WS_FolioSII, CDITotal, CDIIVA, CDIILA, CodCC, CabOpeNumero) " \
                                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s)"
       val = (Numero_GD, Fecha_GD, CodCli_GD, NumPedido, NFicha_GD, TipFicha, Moneda, Cod_Banco, Pto_Emb, Pto_Dest,
              Nave, Contenedor, Num_Sello, Pat_Camion, Chofer, Hora_Salida, Neto_GD, Iva_GD, Ila_GD, Total_GD,
              DeTraslado, NumTraslado, Observacion, Impresa, Cod_Oper, Nula, Factura, CtoVenta, TotalLitros,
              CodBod, FormaPagoGlosa, CodCtaCtble, CodigoEmp, SucCodigo, CorrEmpresa, PesoNeto, PesoBruto,
              TotalCajas, TotalCajasLotes, FormaPagoCodigo, Camion, Carro, Pat_Carro, Referencia, Referencia2,
              CodBodtraslado, Tipo_Doc, Mes, ModificaStock, ParaNCredCpra, TieneNCredCpra, Centralizada,
              MotivoAnulacion, Semana, Serie, Bruto, Tara, Densidad, LtsCamion, CodigoOC, AutorizadoPor,
              ClieBloqueado, IndicadorMora, SaldoCredito, Estado, FechaAutoriza, ConCorrPreliminar, WS_NumCabID,
              WS_FolioSII, CDITotal, CDIIVA, CDIILA, CodCC, CabOpeNumero)
       bdg_cursor.execute(sql, val)
       bdg.commit()
    print("Cantidad de registros en la tabla bdg_guia_despacho: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'guia_despacho', 'bdg_guia_despacho', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_apelacion 57
    i = 0
    kupay_cursor.execute("SELECT codigo, apelacion FROM apelacion")
    registrosorigen = kupay_cursor.rowcount
    print("(57) tabla bdg_apelacion")
    for codigo, apelacion in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_apelacion(codigo, apelacion) VALUES (%s, %s)"
        val = (codigo, apelacion)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_apelacion: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'apelacion', 'bdg_apelacion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Muestra fecha y hora actual al finalizar el proceso
    localtime2 = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de finalizacion del proceso")
    print(localtime2)

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg_cursor.close()
    bdg.close()
    print("fin cierre de cursores y bases")