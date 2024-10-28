import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

def main():
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
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_gcom")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_guia_comp")
        print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso")
        print(localtime)

        ######################
        # Cargar tablas
        ######################

        # TABLA bdg_prod_liqu 55
        i = 0
        kupay_cursor.execute("select NumGuia, CodProv, Fecha, CodUnicoGC, Num_ODC, NomProv, CodBod, Factura, "
                             "Total, TipoDoc, CodMon, Tasa, GC_Glosa, CodOper, CodCC, Centralizada, "
                             "CodigoOC, CDIFactPorRec, CabLlgId from guia_comp")
        registrosorigen = kupay_cursor.rowcount
        print("(62) tabla bdg_guia_comp")
        for NumGuia, CodProv, Fecha, CodUnicoGC, Num_ODC, NomProv, CodBod, Factura, Total, \
            TipoDoc, CodMon, Tasa, GC_Glosa, CodOper, CodCC, Centralizada, CodigoOC, \
            CDIFactPorRec, CabLlgId in kupay_cursor.fetchall():
            i = i + 1
            if str(Tasa) == 'inf':
                Tasa = 0
            if str(Tasa) == '-inf':
                Tasa = 0
            if str(Total) == 'inf':
                Total = 0
            if str(Total) == '-inf':
                Total = 0
            sql = "INSERT INTO " + EsquemaBD + ".bdg_guia_comp (NumGuia, CodProv, Fecha, CodUnicoGC, Num_ODC, " \
                                               "NomProv, CodBod, Factura, Total, TipoDoc, CodMon, Tasa, GC_Glosa," \
                                               "CodOper, CodCC, Centralizada, CodigoOC, CDIFactPorRec, CabLlgId)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s)"
            val = (NumGuia, CodProv, Fecha, CodUnicoGC, Num_ODC, NomProv, CodBod, Factura, Total,
                   TipoDoc, CodMon, Tasa, GC_Glosa, CodOper, CodCC, Centralizada, CodigoOC, CDIFactPorRec, CabLlgId)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_guia_comp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'guia_comp', 'bdg_guia_comp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print(localtime)

        ######################
        # Cargar tablas
        ######################

        # TABLA bdg_prod_liqu 55
        i = 0
        kupay_cursor.execute("select CodUnicoGC, Cant, Item, Valor, Tipo, STotal, Codigo_gcom, Num_ODC, CodBod, QODC, "
                             "ValorMon, Unidad, CodExt, CodCtaCtble, CodCC, CImputacion, NLineaODC, EnviadaF700, "
                             "OrigenDetId, Facturado, NumFC from detalle_gcom")
        registrosorigen = kupay_cursor.rowcount
        print("(61) tabla bdg_detalle_gcom")
        for CodUnicoGC, Cant, Item, Valor, Tipo, STotal, Codigo_gcom, Num_ODC, CodBod, QODC, ValorMon, \
            Unidad, CodExt, CodCtaCtble, CodCC, CImputacion, NLineaODC, EnviadaF700, OrigenDetId, \
            Facturado, NumFC in kupay_cursor.fetchall():
            i = i + 1
            if str(Valor) == 'inf':
                Valor = 0
            if str(Valor) == '-inf':
                Valor = 0
            if str(STotal) == 'inf':
                STotal = 0
            if str(STotal) == '-inf':
                STotal = 0

            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_gcom (CodUnicoGC, Cant, Item, Valor, Tipo, STotal, Codigo_gcom, " \
                                               "Num_ODC, CodBod, QODC, ValorMon, Unidad, CodExt, CodCtaCtble, CodCC, CImputacion, " \
                                               "NLineaODC, EnviadaF700, OrigenDetId, Facturado, NumFC)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodUnicoGC, Cant, Item, Valor, Tipo, STotal, Codigo_gcom, Num_ODC, CodBod, QODC, ValorMon,
                   Unidad, CodExt, CodCtaCtble, CodCC, CImputacion, NLineaODC, EnviadaF700, OrigenDetId, Facturado, NumFC)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_detalle_gcom: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalle_gcom', 'bdg_detalle_gcom', registrosorigen, i, fechacarga)
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

if __name__ == "__main__":
    main()