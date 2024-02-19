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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_facturacompra")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallefactcompra")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detaliq_odc")

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_facturacompra
    kupay_cursor.execute('SELECT NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, MueveExistencia, TipoExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoExentoFC, NetoFCNota, numperiodo, CodigoOC, Desde_Campos, MontoExentoRecuperable, TipoMixtaFC, ConNota, NumFolio, Serie, NSucArg, NFactArg, FechaModificacion, QuienModificacion, AfectoFC, ExentoFC, Centralizada, ConDebito, FechaRecepcion, IlaFC, AfectoIlaFC, CDITotal, CDIIVA, CDIILA, CDIIEsp, CDIIEspNR, CDIProvNC, CDIFactPorRec, CabLlgId FROM facturacompra')
    print("(60) tabla bdg_facturacompra")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, MueveExistencia, TipoExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoExentoFC, NetoFCNota, numperiodo, CodigoOC, Desde_Campos, MontoExentoRecuperable, TipoMixtaFC, ConNota, NumFolio, Serie, NSucArg, NFactArg, FechaModificacion, QuienModificacion, AfectoFC, ExentoFC, Centralizada, ConDebito, FechaRecepcion, IlaFC, AfectoIlaFC, CDITotal, CDIIVA, CDIILA, CDIIEsp, CDIIEspNR, CDIProvNC, CDIFactPorRec, CabLlgId in kupay_cursor.fetchall():
        i = i + 1
        if str(TasaFC) == 'inf':
            TasaFC = 0
        sql = "INSERT INTO " + EsquemaBD + ".bdg_facturacompra (NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, MueveExistencia, TipoExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoExentoFC, NetoFCNota, numperiodo, CodigoOC, Desde_Campos, MontoExentoRecuperable, TipoMixtaFC, ConNota, NumFolio, Serie, NSucArg, NFactArg, FechaModificacion, QuienModificacion, AfectoFC, ExentoFC, Centralizada, ConDebito, FechaRecepcion, IlaFC, AfectoIlaFC, CDITotal, CDIIVA, CDIILA, CDIIEsp, CDIIEspNR, CDIProvNC, CDIFactPorRec, CabLlgId) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, MueveExistencia, TipoExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoExentoFC, NetoFCNota, numperiodo, CodigoOC, Desde_Campos, MontoExentoRecuperable, TipoMixtaFC, ConNota, NumFolio, Serie, NSucArg, NFactArg, FechaModificacion, QuienModificacion, AfectoFC, ExentoFC, Centralizada, ConDebito, FechaRecepcion, IlaFC, AfectoIlaFC, CDITotal, CDIIVA, CDIILA, CDIIEsp, CDIIEspNR, CDIProvNC, CDIFactPorRec, CabLlgId)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_facturacompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'facturacompra', 'bdg_facturacompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # TABLA bdg_detallefactcompra
    kupay_cursor.execute('SELECT CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Describe, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, ValorImpuesto, IDDetFactCpra, Item, ConDetalle, CodImpuesto, TotalImpuesto, ValorCompra, ValorNota, SubTotalNota, Afecto, ConILA, SubTotalAfecto, SubTotalExento, SubTotalAfectoIla, CImputacion, NLineaODC, EnviadaF700, OrigenDetId FROM detallefactcompra')
    print("(61) tabla bdg_detallefactcompra")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Describe, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, ValorImpuesto, IDDetFactCpra, Item, ConDetalle, CodImpuesto, TotalImpuesto, ValorCompra, ValorNota, SubTotalNota, Afecto, ConILA, SubTotalAfecto, SubTotalExento, SubTotalAfectoIla, CImputacion, NLineaODC, EnviadaF700, OrigenDetId in kupay_cursor.fetchall():
        i = i + 1
        Describe_det = Describe
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detallefactcompra (CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Describe_det, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, ValorImpuesto, IDDetFactCpra, Item, ConDetalle, CodImpuesto, TotalImpuesto, ValorCompra, ValorNota, SubTotalNota, Afecto, ConILA, SubTotalAfecto, SubTotalExento, SubTotalAfectoIla, CImputacion, NLineaODC, EnviadaF700, OrigenDetId) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Describe_det, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, ValorImpuesto, IDDetFactCpra, Item, ConDetalle, CodImpuesto, TotalImpuesto, ValorCompra, ValorNota, SubTotalNota, Afecto, ConILA, SubTotalAfecto, SubTotalExento, SubTotalAfectoIla, CImputacion, NLineaODC, EnviadaF700, OrigenDetId)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_detallefactcompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallefactcompra', 'bdg_detallefactcompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # TABLA DetaLiq_ODC
    kupay_cursor.execute('SELECT Liq_Correlativo, DL_Codigo, DL_Descripcion, DL_Cantidad, DL_Unidad, DL_Tipo, NLineaODC, NLineaSC FROM DetaLiq_ODC')
    print("(62) tabla DetaLiq_ODC")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for Liq_Correlativo, DL_Codigo, DL_Descripcion, DL_Cantidad, DL_Unidad, DL_Tipo, NLineaODC, NLineaSC in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_DetaLiq_ODC (Liq_Correlativo, DL_Codigo, DL_Descripcion, DL_Cantidad, DL_Unidad, DL_Tipo, NLineaODC, NLineaSC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Liq_Correlativo, DL_Codigo, DL_Descripcion, DL_Cantidad, DL_Unidad, DL_Tipo, NLineaODC, NLineaSC)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla DetaLiq_ODC: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'DetaLiq_ODC', 'bdg_DetaLiq_ODC', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")