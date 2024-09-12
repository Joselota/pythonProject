import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage


def envio_mail(v_email_subject):
    email_subject = v_email_subject
    message = EmailMessage()
    message['Subject'] = email_subject
    message['From'] = sender_email
    message['To'] = 'igonzalez@viumanent.cl'
    message.set_content("Aviso termino de ejecución script")
    server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
    server.ehlo()  # Identify this client to the SMTP server
    server.starttls()  # Secure the SMTP connection
    server.login(sender_email, email_pass)  # Login to email account
    server.send_message(message)  # Send email
    server.quit()  # Close connection to serve


def main():
    # do whatever the script does
    print("CARGACAMPOS 1 MODULO")
    # VariablesGlobales
    EsquemaBD = "stagecampos"
    SistemaOrigen = "Campos"
    fechacarga = datetime.now()

    # Generando identificador para proceso de cuadratura
    dia = str(100 + int(format(fechacarga.day)))
    mes = str(100 + int(format(fechacarga.month)))
    agno = format(fechacarga.year)
    Identificador = str(agno) + str(mes[1:]) + str(dia[1:])

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bodegas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bonos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".afp")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".alimentos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".anticipos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".asignaciondiaria")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".asignagastos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".auditoria")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".billetes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cierreanual")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".centrocosto")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cargas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cuartel")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cosecha")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".correlativos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".condicionpago")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".comprobantes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cultivos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cultivoscuartel")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".dctosper")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".descuentos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallebonoprodmes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallecomprobante")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallecomprobantecuartel")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallecontratista")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallefactcompra")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detalleliqodc")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detalleliquidacion")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detalleodc")

    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()

    # TABLA bodegas
    i = 0
    campos_cursor.execute('SELECT BdgCodigo, BdgNombre, ConStock, Cerrada FROM bodegas')
    registrosorigen = campos_cursor.rowcount
    print("(5) tabla bodegas")
    print(registrosorigen)
    for BdgCodigo, BdgNombre, ConStock, Cerrada in campos_cursor.fetchall():
        i = i + 1
        print(BdgCodigo, BdgNombre, ConStock, Cerrada)
        sql = "INSERT INTO " + EsquemaBD + ".bodegas(BdgCodigo, BdgNombre, ConStock, Cerrada) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (BdgCodigo, BdgNombre, ConStock, Cerrada)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla bodegas: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'bodegas', 'bodegas', registrosorigen, i, fechacarga)
    print(sql, val)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla DetalleODC
    i = 0
    campos_cursor.execute('SELECT `NumODC`, `Cantidad`, `Describe`, `Valor`, `Sub_Total`, '
                          '`Codigo`, `Tipo`, `Recibido`, `PorRecibir`, `Stock`, `SaldoODC`, '
                          '`Unidad`, `FechaRecepcion`, `PorcDcto`, `ValorMinimo`, `NLinea`, '
                          '`TieneODC`, `NLineaSC`, `CodServicio` FROM DetalleODC')
    registrosorigen = campos_cursor.rowcount
    print("(1) tabla DetalleODC")
    print(registrosorigen)
    for NumODC, Cantidad, Descripcion, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, Fevarcharecepcion, PorcDcto, ValorMinimo, NLinea, TieneODC, NLineaSC, CodServicio in campos_cursor.fetchall():
        i = i + 1
        if Fevarcharecepcion == None:
            Fevarcharecepcion2 = None
        else:
            Fevarcharecepcion2 = Fevarcharecepcion
        print(NumODC, Cantidad, Descripcion, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC,
              Unidad, Fevarcharecepcion2, PorcDcto, ValorMinimo, NLinea, TieneODC, NLineaSC, CodServicio)
        sql = "INSERT INTO " + EsquemaBD + ".DetalleODC (NumODC, Cantidad, Descripcion, Valor, Sub_Total, " \
                                           "Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, " \
                                           "Fevarcharecepcion, PorcDcto, ValorMinimo, NLinea, TieneODC, " \
                                           "NLineaSC, CodServicio) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        NumODC, Cantidad, Descripcion, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad,
        Fevarcharecepcion2, PorcDcto, ValorMinimo, NLinea, TieneODC, NLineaSC, CodServicio)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla DetalleODC: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'DetalleODC', 'DetalleODC', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detalleliquidacion
    i = 0
    campos_cursor.execute('SELECT CodLiq, CodLabor, TotalValor, TotalTrato, TotalDias, '
                          'ValorDia, CodCuartel, CodPer, PagaTrato, Bonos '
                          'FROM detalleliquidacion')
    registrosorigen = campos_cursor.rowcount
    print("(2) tabla detalleliquidacion")
    print(registrosorigen)
    for CodLiq, CodLabor, TotalValor, TotalTrato, TotalDias, ValorDia, CodCuartel, CodPer, PagaTrato, Bonos in campos_cursor.fetchall():
        i = i + 1
        if str(TotalValor) == 'inf':
            TotalValor = 0
        if str(ValorDia) == 'inf':
            ValorDia = 0
        print(CodLiq, CodLabor, TotalValor, TotalTrato, TotalDias, ValorDia, CodCuartel, CodPer, PagaTrato, Bonos)
        sql = "INSERT INTO " + EsquemaBD + ".detalleliquidacion(CodLiq, CodLabor, TotalValor, " \
                                           "TotalTrato, TotalDias, ValorDia, CodCuartel, CodPer, " \
                                           "PagaTrato, Bonos) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodLiq, CodLabor, TotalValor, TotalTrato, TotalDias, ValorDia, CodCuartel, CodPer, PagaTrato, Bonos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detalleliquidacion: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalleliquidacion', 'detalleliquidacion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detalleliqodc
    i = 0
    campos_cursor.execute('SELECT `LiqODCCorr`, `Codigo`, `Describe`, `Cantidad`, '
                          '`Unidad`, `Tipo`, `NLineaODC` FROM detalleliqodc')
    registrosorigen = campos_cursor.rowcount
    print("(3) tabla detalleliqodc")
    print(registrosorigen)
    for LiqODCCorr, Codigo, Descripcion, Cantidad, Unidad, Tipo, NLineaODC in campos_cursor.fetchall():
        i = i + 1
        print(LiqODCCorr, Codigo, Descripcion, Cantidad, Unidad, Tipo, NLineaODC)
        sql = "INSERT INTO " + EsquemaBD + ".detalleliqodc(LiqODCCorr, Codigo, Descripcion, " \
                                           "Cantidad, Unidad, Tipo, NLineaODC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (LiqODCCorr, Codigo, Descripcion, Cantidad, Unidad, Tipo, NLineaODC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detalleliqodc: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalleliqodc', 'detalleliqodc', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA comprobantes
    i = 0
    campos_cursor.execute('SELECT CodCom, Factura, Guia, Fecha, CodProv, Vale, Tipo, Prod, '
                          'SaldoComp, TotalComp, FechaVence, TotAbonos, NetoComp, IVAComp, '
                          'BdgCodigo, ImpEspecifico, FechaImputa, BoletaHono, ExentaImpEsp, Nota_C_D, '
                          'NumODC, ValorTasa, CodMoneda, CodCC, ImpuestoExento, NumExportacion, '
                          'NumPeriodo, GlosaComp, Origen, CodOper, TipoDocumento, BdgDestino, '
                          'MueveStock, FueContabilizada, OperacionCtbleComp, EsExenta, EsAfecta, '
                          'ExentoComp, AfectoComp, IvaNoRec, ParaNota, TieneNota, TipoNota, '
                          'CentralizadaCons, EsReversaKPY FROM comprobantes')
    registrosorigen = campos_cursor.rowcount
    print("(4) tabla comprobantes")
    print(registrosorigen)
    for CodCom, Factura, Guia, Fecha, CodProv, Vale, Tipo, Prod, SaldoComp, TotalComp, FechaVence, TotAbonos, NetoComp, IVAComp, BdgCodigo, ImpEspecifico, FechaImputa, BoletaHono, ExentaImpEsp, Nota_C_D, NumODC, ValorTasa, CodMoneda, CodCC, ImpuestoExento, NumExportacion, NumPeriodo, GlosaComp, Origen, CodOper, TipoDocumento, BdgDestino, MueveStock, FueContabilizada, OperacionCtbleComp, EsExenta, EsAfecta, ExentoComp, AfectoComp, IvaNoRec, ParaNota, TieneNota, TipoNota, CentralizadaCons, EsReversaKPY in campos_cursor.fetchall():
        i = i + 1
        if Fecha == None:
            Fecha1 = None
        else:
            Fecha1 = Fecha
        if FechaVence == None:
            FechaVence2 = None
        else:
            FechaVence2 = FechaVence
        print(type(FechaImputa))
        if FechaImputa == None:
            FechaImputa2 = None
        else:
            if FechaImputa.year == 3201:
                FechaImputa2 = None
            else:
                FechaImputa2 = FechaImputa
        print(CodCom, Factura, Guia, Fecha1, CodProv, Vale, Tipo, Prod, SaldoComp, TotalComp, FechaVence2, TotAbonos,
              NetoComp, IVAComp, BdgCodigo, ImpEspecifico, FechaImputa2, BoletaHono, ExentaImpEsp, Nota_C_D, NumODC,
              ValorTasa, CodMoneda, CodCC, ImpuestoExento, NumExportacion, NumPeriodo, GlosaComp, Origen, CodOper,
              TipoDocumento, BdgDestino, MueveStock, FueContabilizada, OperacionCtbleComp, EsExenta, EsAfecta,
              ExentoComp, AfectoComp, IvaNoRec, ParaNota, TieneNota, TipoNota, CentralizadaCons, EsReversaKPY)
        sql = "INSERT INTO " + EsquemaBD + ".comprobantes(CodCom, Factura, Guia, Fecha, CodProv, Vale, " \
                                           "Tipo, Prod, SaldoComp, TotalComp, FechaVence, TotAbonos, NetoComp, " \
                                           "IVAComp, BdgCodigo, ImpEspecifico, FechaImputa, BoletaHono, " \
                                           "ExentaImpEsp, Nota_C_D, NumODC, ValorTasa, CodMoneda, CodCC, " \
                                           "ImpuestoExento, NumExportacion, NumPeriodo, GlosaComp, Origen, " \
                                           "CodOper, TipoDocumento, BdgDestino, MueveStock, FueContabilizada, " \
                                           "OperacionCtbleComp, EsExenta, EsAfecta, ExentoComp, AfectoComp, " \
                                           "IvaNoRec, ParaNota, TieneNota, TipoNota, CentralizadaCons, EsReversaKPY) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodCom, Factura, Guia, Fecha1, CodProv, Vale, Tipo, Prod, SaldoComp, TotalComp, FechaVence2, TotAbonos,
               NetoComp, IVAComp, BdgCodigo, ImpEspecifico, FechaImputa2, BoletaHono, ExentaImpEsp, Nota_C_D, NumODC,
               ValorTasa, CodMoneda, CodCC, ImpuestoExento, NumExportacion, NumPeriodo, GlosaComp, Origen, CodOper,
               TipoDocumento, BdgDestino, MueveStock, FueContabilizada, OperacionCtbleComp, EsExenta, EsAfecta,
               ExentoComp, AfectoComp, IvaNoRec, ParaNota, TieneNota, TipoNota, CentralizadaCons, EsReversaKPY)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla comprobantes: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'comprobantes', 'comprobantes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA Bonos
    i = 0
    campos_cursor.execute('SELECT CodBono, NomBono, ValorBono, AfectaSC, FormulaRem700 FROM bonos')
    registrosorigen = campos_cursor.rowcount
    print("(6) tabla bodegas")
    print(registrosorigen)
    for CodBono, NomBono, ValorBono, AfectaSC, FormulaRem700 in campos_cursor.fetchall():
        i = i + 1
        print(CodBono, NomBono, ValorBono, AfectaSC, FormulaRem700)
        sql = "INSERT INTO " + EsquemaBD + ".bonos(CodBono, NomBono, ValorBono, AfectaSC, FormulaRem700) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (CodBono, NomBono, ValorBono, AfectaSC, FormulaRem700)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla bonos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'bonos', 'bonos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA afp
    i = 0
    campos_cursor.execute('SELECT CodAfp, NomAfp, DctoAfp, CodSuperInt, TipoPrevision FROM afp')
    registrosorigen = campos_cursor.rowcount
    print("(7) tabla afp")
    print(registrosorigen)
    for CodAfp, NomAfp, DctoAfp, CodSuperInt, TipoPrevision in campos_cursor.fetchall():
        i = i + 1
        print(CodAfp, NomAfp, DctoAfp, CodSuperInt, TipoPrevision)
        sql = "INSERT INTO " + EsquemaBD + ".afp(CodAfp, NomAfp, DctoAfp, CodSuperInt, TipoPrevision) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (CodAfp, NomAfp, DctoAfp, CodSuperInt, TipoPrevision)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla afp: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'afp', 'afp', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA alimentos
    i = 0
    campos_cursor.execute('SELECT CodAlimento, NomAlimento FROM alimentos')
    registrosorigen = campos_cursor.rowcount
    print("(8) tabla alimentos")
    print(registrosorigen)
    for CodAlimento, NomAlimento in campos_cursor.fetchall():
        i = i + 1
        print(CodAlimento, NomAlimento)
        sql = "INSERT INTO " + EsquemaBD + ".alimentos(CodAlimento, NomAlimento) " \
                                           "VALUES (%s, %s)"
        val = (CodAlimento, NomAlimento)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla alimentos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'alimentos', 'alimentos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA anticipos
    i = 0
    campos_cursor.execute('SELECT CodPer, Fecha, Valor, Porcentaje, CodPeriodo, Folio, CodUnicoFundo FROM anticipos')
    registrosorigen = campos_cursor.rowcount
    print("(9) tabla anticipos")
    print(registrosorigen)
    for CodPer, Fecha, Valor, Porcentaje, CodPeriodo, Folio, CodUnicoFundo in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, Fecha, Valor, Porcentaje, CodPeriodo, Folio, CodUnicoFundo)
        sql = "INSERT INTO " + EsquemaBD + ".anticipos(CodPer, Fecha, Valor, Porcentaje, CodPeriodo, Folio, CodUnicoFundo) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (CodPer, Fecha, Valor, Porcentaje, CodPeriodo, Folio, CodUnicoFundo)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla anticipos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'anticipos', 'anticipos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA asignaciondiaria
    i = 0
    campos_cursor.execute('SELECT CodPer, Fecha, Desayuno, Almuerzo, Once, Comida, CodLabor, '
                          'CodFaena, CostoLabor, NomPersona, Colacion, CodContratista, CodConcesionario, '
                          'IDLiquidacion, TrabajadorEmpresa, Asistencia, IDDetalleContrato, ContratoTexto, '
                          'Bono, HoraExtra, TipoAlimentacion, IDAsignacionDiaria, UnicoFundo, Selec, '
                          'TipoHexCttsta, CodBonoEmpCtta, Folio FROM asignaciondiaria')
    registrosorigen = campos_cursor.rowcount
    print("(10) tabla asignaciondiaria")
    print(registrosorigen)
    for CodPer, Fecha, Desayuno, Almuerzo, Once, Comida, CodLabor, CodFaena, CostoLabor, NomPersona, Colacion, CodContratista, CodConcesionario, IDLiquidacion, TrabajadorEmpresa, Asistencia, IDDetalleContrato, ContratoTexto, Bono, HoraExtra, TipoAlimentacion, IDAsignacionDiaria, UnicoFundo, Selec, TipoHexCttsta, CodBonoEmpCtta, Folio in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, Fecha, Desayuno, Almuerzo, Once, Comida, CodLabor, CodFaena, CostoLabor, NomPersona, Colacion,
              CodContratista, CodConcesionario, IDLiquidacion, TrabajadorEmpresa, Asistencia, IDDetalleContrato,
              ContratoTexto, Bono, HoraExtra, TipoAlimentacion, IDAsignacionDiaria, UnicoFundo, Selec, TipoHexCttsta,
              CodBonoEmpCtta, Folio)
        sql = "INSERT INTO " + EsquemaBD + ".asignaciondiaria(CodPer, Fecha, Desayuno, Almuerzo, Once, " \
                                           "Comida, CodLabor, CodFaena, CostoLabor, NomPersona, Colacion, " \
                                           "CodContratista, CodConcesionario, IDLiquidacion, TrabajadorEmpresa, " \
                                           "Asistencia, IDDetalleContrato, ContratoTexto, Bono, HoraExtra, " \
                                           "TipoAlimentacion, IDAsignacionDiaria, UnicoFundo, Selec, TipoHexCttsta, " \
                                           "CodBonoEmpCtta, Folio) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodPer, Fecha, Desayuno, Almuerzo, Once, Comida, CodLabor, CodFaena, CostoLabor, NomPersona, Colacion,
               CodContratista, CodConcesionario, IDLiquidacion, TrabajadorEmpresa, Asistencia, IDDetalleContrato,
               ContratoTexto, Bono, HoraExtra, TipoAlimentacion, IDAsignacionDiaria, UnicoFundo, Selec, TipoHexCttsta,
               CodBonoEmpCtta, Folio)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Cantidad de registros en la tabla asignaciondiaria: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'asignaciondiaria', 'asignaciondiaria', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA asignagastos
    i = 0
    campos_cursor.execute('SELECT CodCom, Fecha, CodSer, Costo, Sub_Total, CodCuartel, MesAgno, NomSer, CodCtaCtble, '
                          'Cantidad, IdDetAsigGto, CodFaena FROM asignagastos')
    registrosorigen = campos_cursor.rowcount
    print("(11) tabla asignagastos")
    print(registrosorigen)
    for CodCom, Fecha, CodSer, Costo, Sub_Total, CodCuartel, MesAgno, NomSer, CodCtaCtble, Cantidad, IdDetAsigGto, CodFaena in campos_cursor.fetchall():
        i = i + 1
        print(CodCom, Fecha, CodSer, Costo, Sub_Total, CodCuartel, MesAgno, NomSer, CodCtaCtble, Cantidad, IdDetAsigGto,
              CodFaena)
        sql = "INSERT INTO " + EsquemaBD + ".asignagastos(CodCom, Fecha, CodSer, Costo, Sub_Total, CodCuartel, " \
                                           "MesAgno, NomSer, CodCtaCtble, Cantidad, IdDetAsigGto, CodFaena) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        CodCom, Fecha, CodSer, Costo, Sub_Total, CodCuartel, MesAgno, NomSer, CodCtaCtble, Cantidad, IdDetAsigGto,
        CodFaena)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla asignagastos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'asignagastos', 'asignagastos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA auditoria
    i = 0
    campos_cursor.execute('SELECT Fecha, Hora, Codcom, TipoDoc, UserName, GlosaA, PrdCod FROM auditoria')
    registrosorigen = campos_cursor.rowcount
    print("(12) tabla auditoria")
    print(registrosorigen)
    for Fecha, Hora, Codcom, TipoDoc, UserName, GlosaA, PrdCod in campos_cursor.fetchall():
        i = i + 1
        print(Fecha, Hora, Codcom, TipoDoc, UserName, GlosaA, PrdCod)
        sql = "INSERT INTO " + EsquemaBD + ".auditoria(Fecha, Hora, Codcom, TipoDoc, UserName, GlosaA, PrdCod) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Fecha, Hora, Codcom, TipoDoc, UserName, GlosaA, PrdCod)
        print(sql, val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla auditoria: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'auditoria', 'auditoria', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA billetes
    i = 0
    campos_cursor.execute('SELECT ValorBillete, NomBillete, CantBillete FROM billetes')
    registrosorigen = campos_cursor.rowcount
    print("(13) tabla billetes")
    print(registrosorigen)
    for ValorBillete, NomBillete, CantBillete in campos_cursor.fetchall():
        i = i + 1
        print(ValorBillete, NomBillete, CantBillete)
        sql = "INSERT INTO " + EsquemaBD + ".billetes(ValorBillete, NomBillete, CantBillete) " \
                                           "VALUES (%s, %s, %s)"
        val = (ValorBillete, NomBillete, CantBillete)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla billetes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'billetes', 'billetes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA cargas
    i = 0
    campos_cursor.execute('SELECT CodPer, CodCarga, ValorCarga, NumCargas, TotalCargas FROM cargas')
    registrosorigen = campos_cursor.rowcount
    print("(14) tabla cargas")
    print(registrosorigen)
    for CodPer, CodCarga, ValorCarga, NumCargas, TotalCargas in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, CodCarga, ValorCarga, NumCargas, TotalCargas)
        sql = "INSERT INTO " + EsquemaBD + ".cargas(CodPer, CodCarga, ValorCarga, NumCargas, TotalCargas) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (CodPer, CodCarga, ValorCarga, NumCargas, TotalCargas)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cargas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cargas', 'cargas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA centrocosto
    i = 0
    campos_cursor.execute('SELECT CodCC, NomCC, CentroRespFin700, DivisionFin700, CodZona FROM centrocosto')
    registrosorigen = campos_cursor.rowcount
    print("(15) tabla cargas")
    print(registrosorigen)
    for CodCC, NomCC, CentroRespFin700, DivisionFin700, CodZona in campos_cursor.fetchall():
        i = i + 1
        print(CodCC, NomCC, CentroRespFin700, DivisionFin700, CodZona)
        sql = "INSERT INTO " + EsquemaBD + ".centrocosto(CodCC, NomCC, CentroRespFin700, DivisionFin700, CodZona) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (CodCC, NomCC, CentroRespFin700, DivisionFin700, CodZona)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla centrocosto: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'centrocosto', 'centrocosto', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA auditoria
    i = 0
    campos_cursor.execute('SELECT CodParce, NomParce, Agno, CodCuartel, NomCuartel, CodCultivo, NomCultivo, '
                          'CodVarie, NomVarie, TotMMOO, TotProd, TotMaqui, TotGastos, TotMMOOInd, CostoTotal, '
                          'Cosecha, IngresosTotal, Utilidad, TotJornHbre, TotComprobantes, TotFacturaDirecta, '
                          'TotContratistas, Tipo FROM cierreanual')
    registrosorigen = campos_cursor.rowcount
    print("(16) tabla cierreanual")
    print(registrosorigen)
    for CodParce, NomParce, Agno, CodCuartel, NomCuartel, CodCultivo, NomCultivo, CodVarie, NomVarie, TotMMOO, TotProd, TotMaqui, TotGastos, TotMMOOInd, CostoTotal, Cosecha, IngresosTotal, Utilidad, TotJornHbre, TotComprobantes, TotFacturaDirecta, TotContratistas, Tipo in campos_cursor.fetchall():
        i = i + 1
        print(CodParce, NomParce, Agno, CodCuartel, NomCuartel, CodCultivo, NomCultivo, CodVarie, NomVarie, TotMMOO,
              TotProd, TotMaqui, TotGastos, TotMMOOInd, CostoTotal, Cosecha, IngresosTotal, Utilidad, TotJornHbre,
              TotComprobantes, TotFacturaDirecta, TotContratistas, Tipo)
        sql = "INSERT INTO " + EsquemaBD + ".cierreanual(CodParce, NomParce, Agno, CodCuartel, NomCuartel, " \
                                           "CodCultivo, NomCultivo, CodVarie, NomVarie, TotMMOO, TotProd, " \
                                           "TotMaqui, TotGastos, TotMMOOInd, CostoTotal, Cosecha, IngresosTotal, " \
                                           "Utilidad, TotJornHbre, TotComprobantes, TotFacturaDirecta, " \
                                           "TotContratistas, Tipo) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        CodParce, NomParce, Agno, CodCuartel, NomCuartel, CodCultivo, NomCultivo, CodVarie, NomVarie, TotMMOO, TotProd,
        TotMaqui, TotGastos, TotMMOOInd, CostoTotal, Cosecha, IngresosTotal, Utilidad, TotJornHbre, TotComprobantes,
        TotFacturaDirecta, TotContratistas, Tipo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cierreanual: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cierreanual', 'cierreanual', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA condicionpago
    i = 0
    campos_cursor.execute('SELECT CodCondPago, NomCondPago, DiasCondPago FROM condicionpago')
    registrosorigen = campos_cursor.rowcount
    print("(17) tabla condicionpago")
    print(registrosorigen)
    for CodCondPago, NomCondPago, DiasCondPago in campos_cursor.fetchall():
        i = i + 1
        print(CodCondPago, NomCondPago, DiasCondPago)
        sql = "INSERT INTO " + EsquemaBD + ".condicionpago(CodCondPago, NomCondPago, DiasCondPago) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodCondPago, NomCondPago, DiasCondPago)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla condicionpago: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'condicionpago', 'condicionpago', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA correlativos
    i = 0
    campos_cursor.execute('SELECT CodCorrelativo, NumCorrelativo, NomCorrelativo FROM correlativos')
    registrosorigen = campos_cursor.rowcount
    print("(18) tabla correlativos")
    print(registrosorigen)
    for CodCorrelativo, NumCorrelativo, NomCorrelativo in campos_cursor.fetchall():
        i = i + 1
        print(CodCorrelativo, NumCorrelativo, NomCorrelativo)
        sql = "INSERT INTO " + EsquemaBD + ".correlativos(CodCorrelativo, NumCorrelativo, NomCorrelativo) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodCorrelativo, NumCorrelativo, NomCorrelativo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla correlativos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'correlativos', 'correlativos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA cosecha
    i = 0
    campos_cursor.execute('SELECT PrdCod, CodCuartel, CodCultivo, CodVariedad, KilosEnvase, TeCodigo, '
                          'Cantidad, KlsMerma, KilosTotal, NLinea, KlsEnvProc, KlsVenta, '
                          'SaldoKilos FROM cosecha')
    registrosorigen = campos_cursor.rowcount
    print("(19) tabla cosecha")
    print(registrosorigen)
    for PrdCod, CodCuartel, CodCultivo, CodVariedad, KilosEnvase, TeCodigo, Cantidad, KlsMerma, KilosTotal, NLinea, KlsEnvProc, KlsVenta, SaldoKilos in campos_cursor.fetchall():
        i = i + 1
        print(PrdCod, CodCuartel, CodCultivo, CodVariedad, KilosEnvase, TeCodigo, Cantidad, KlsMerma, KilosTotal,
              NLinea, KlsEnvProc, KlsVenta, SaldoKilos)
        sql = "INSERT INTO " + EsquemaBD + ".cosecha(PrdCod, CodCuartel, CodCultivo, CodVariedad, " \
                                           "KilosEnvase, TeCodigo, Cantidad, KlsMerma, KilosTotal, NLinea, " \
                                           "KlsEnvProc, KlsVenta, SaldoKilos) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        PrdCod, CodCuartel, CodCultivo, CodVariedad, KilosEnvase, TeCodigo, Cantidad, KlsMerma, KilosTotal, NLinea,
        KlsEnvProc, KlsVenta, SaldoKilos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cosecha: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cosecha', 'cosecha', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA cuartel
    i = 0
    campos_cursor.execute('SELECT CodCuartel, NomCuartel, CodParcela, HectCuartel, CodCultivo, NPlantas, '
                          'TotPlantas, DistanciaPlantas, FechaInicio, FechaTermino, CodVarPpal, IDNLineaCC, '
                          'FechaValidacion, UnicoCuartel, UnicoFundo, CodEmpresa, CodCtaCtble, PortaInjerto, '
                          'Polinizante, AgnoPlantacion, SistConduccion, PlantacionEntreHilera, '
                          'PlantacionSobreHilera, CodCCFin700, CodSector, CodCC FROM cuartel')
    registrosorigen = campos_cursor.rowcount
    print("(21) tabla cuartel")
    print(registrosorigen)
    for CodCuartel, NomCuartel, CodParcela, HectCuartel, CodCultivo, NPlantas, TotPlantas, DistanciaPlantas, FechaInicio, FechaTermino, CodVarPpal, IDNLineaCC, FechaValidacion, UnicoCuartel, UnicoFundo, CodEmpresa, CodCtaCtble, PortaInjerto, Polinizante, AgnoPlantacion, SistConduccion, PlantacionEntreHilera, PlantacionSobreHilera, CodCCFin700, CodSector, CodCC in campos_cursor.fetchall():
        i = i + 1

        print(CodCuartel, NomCuartel, CodParcela, HectCuartel, CodCultivo, NPlantas, TotPlantas, DistanciaPlantas,
              FechaInicio, FechaTermino, CodVarPpal, IDNLineaCC, FechaValidacion, UnicoCuartel, UnicoFundo, CodEmpresa,
              CodCtaCtble, PortaInjerto, Polinizante, AgnoPlantacion, SistConduccion, PlantacionEntreHilera,
              PlantacionSobreHilera, CodCCFin700, CodSector, CodCC)
        sql = "INSERT INTO " + EsquemaBD + ".cuartel(CodCuartel, NomCuartel, CodParcela, HectCuartel, CodCultivo, " \
                                           "NPlantas, TotPlantas, DistanciaPlantas, FechaInicio, FechaTermino, CodVarPpal, " \
                                           "IDNLineaCC, FechaValidacion, UnicoCuartel, UnicoFundo, CodEmpresa, CodCtaCtble, " \
                                           "PortaInjerto, Polinizante, AgnoPlantacion, SistConduccion, PlantacionEntreHilera," \
                                           "PlantacionSobreHilera, CodCCFin700, CodSector, CodCC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodCuartel, NomCuartel, CodParcela, HectCuartel, CodCultivo, NPlantas, TotPlantas, DistanciaPlantas,
               FechaInicio, FechaTermino, CodVarPpal, IDNLineaCC, FechaValidacion, UnicoCuartel, UnicoFundo, CodEmpresa,
               CodCtaCtble, PortaInjerto, Polinizante, AgnoPlantacion, SistConduccion, PlantacionEntreHilera,
               PlantacionSobreHilera, CodCCFin700, CodSector, CodCC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cuartel: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cuartel', 'cuartel', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA cultivos
    i = 0
    campos_cursor.execute('SELECT CodCultivo, NomCultivo, PorcMaxMerma FROM cultivos')
    registrosorigen = campos_cursor.rowcount
    print("(22) tabla cultivos")
    print(registrosorigen)
    for CodCultivo, NomCultivo, PorcMaxMerma in campos_cursor.fetchall():
        i = i + 1
        print(CodCultivo, NomCultivo, PorcMaxMerma)
        sql = "INSERT INTO " + EsquemaBD + ".cultivos(CodCultivo, NomCultivo, PorcMaxMerma) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodCultivo, NomCultivo, PorcMaxMerma)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cultivos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cultivos', 'cultivos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA cultivos
    i = 0
    campos_cursor.execute('SELECT CodCuartel, CodVariedad, PorcVariedad, UnicoCC FROM cultivoscuartel')
    registrosorigen = campos_cursor.rowcount
    print("(23) tabla cultivoscuartel")
    print(registrosorigen)
    for CodCuartel, CodVariedad, PorcVariedad, UnicoCC in campos_cursor.fetchall():
        i = i + 1
        print(CodCuartel, CodVariedad, PorcVariedad, UnicoCC)
        sql = "INSERT INTO " + EsquemaBD + ".cultivoscuartel(CodCuartel, CodVariedad, PorcVariedad, UnicoCC) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (CodCuartel, CodVariedad, PorcVariedad, UnicoCC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla cultivoscuartel: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cultivoscuartel', 'cultivoscuartel', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA dctosper
    i = 0
    campos_cursor.execute('SELECT CodPer, CodDcto, ValorDcto FROM dctosper')
    registrosorigen = campos_cursor.rowcount
    print("(24) tabla dctosper")
    print(registrosorigen)
    for CodPer, CodDcto, ValorDcto in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, CodDcto, ValorDcto)
        sql = "INSERT INTO " + EsquemaBD + ".dctosper(CodPer, CodDcto, ValorDcto) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodPer, CodDcto, ValorDcto)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla dctosper: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'dctosper', 'dctosper', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA descuentos
    i = 0
    campos_cursor.execute('SELECT CodDcto, NomDcto, ValorDcto, Formula FROM descuentos')
    registrosorigen = campos_cursor.rowcount
    print("(25) tabla descuentos")
    print(registrosorigen)
    for CodDcto, NomDcto, ValorDcto, Formula in campos_cursor.fetchall():
        i = i + 1
        print(CodDcto, NomDcto, ValorDcto, Formula)
        sql = "INSERT INTO " + EsquemaBD + ".descuentos(CodDcto, NomDcto, ValorDcto, Formula) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (CodDcto, NomDcto, ValorDcto, Formula)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla descuentos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'descuentos', 'descuentos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallebonoprodmes
    i = 0
    campos_cursor.execute('SELECT CodMes, CodPer, Folio, Mes, Agno, CodPeriodo, CodLabor, DiasTrabajados, '
                          'CantidadMes, CantidadMinima, Tarifa, CantidadBono, MontoBono FROM detallebonoprodmes')
    registrosorigen = campos_cursor.rowcount
    print("(26) tabla detallebonoprodmes")
    print(registrosorigen)
    for CodMes, CodPer, Folio, Mes, Agno, CodPeriodo, CodLabor, DiasTrabajados, CantidadMes, CantidadMinima, Tarifa, CantidadBono, MontoBono in campos_cursor.fetchall():
        i = i + 1
        print(CodMes, CodPer, Folio, Mes, Agno, CodPeriodo, CodLabor, DiasTrabajados, CantidadMes, CantidadMinima,
              Tarifa, CantidadBono, MontoBono)
        sql = "INSERT INTO " + EsquemaBD + ".detallebonoprodmes(CodMes, CodPer, Folio, Mes, Agno, CodPeriodo, " \
                                           "CodLabor, DiasTrabajados, CantidadMes, CantidadMinima, Tarifa, " \
                                           "CantidadBono, MontoBono) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        CodMes, CodPer, Folio, Mes, Agno, CodPeriodo, CodLabor, DiasTrabajados, CantidadMes, CantidadMinima, Tarifa,
        CantidadBono, MontoBono)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallebonoprodmes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallebonoprodmes', 'detallebonoprodmes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallecomprobante
    i = 0
    campos_cursor.execute(
        'SELECT CodCom, Referencia, Costo, ConDetalle, CodCtaCtble, IDDetComp FROM detallecomprobante')
    registrosorigen = campos_cursor.rowcount
    print("(27) tabla detallecomprobante")
    print(registrosorigen)
    for CodCom, Referencia, Costo, ConDetalle, CodCtaCtble, IDDetComp in campos_cursor.fetchall():
        i = i + 1
        print(CodCom, Referencia, Costo, ConDetalle, CodCtaCtble, IDDetComp)
        sql = "INSERT INTO " + EsquemaBD + ".detallecomprobante(CodCom, Referencia, Costo, ConDetalle, CodCtaCtble, IDDetComp) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (CodCom, Referencia, Costo, ConDetalle, CodCtaCtble, IDDetComp)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallecomprobante: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallecomprobante', 'detallecomprobante', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallecomprobantecuartel
    i = 0
    campos_cursor.execute('SELECT IDDetComp, CodCuartel, Costo, Fecha FROM detallecomprobantecuartel')
    registrosorigen = campos_cursor.rowcount
    print("(28) tabla detallecomprobantecuartel")
    print(registrosorigen)
    for IDDetComp, CodCuartel, Costo, Fecha in campos_cursor.fetchall():
        i = i + 1
        print(IDDetComp, CodCuartel, Costo, Fecha)
        sql = "INSERT INTO " + EsquemaBD + ".detallecomprobantecuartel(IDDetComp, CodCuartel, Costo, Fecha) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (IDDetComp, CodCuartel, Costo, Fecha)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallecomprobantecuartel: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallecomprobantecuartel', 'detallecomprobantecuartel', registrosorigen, i,
           fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallecontratista
    i = 0
    campos_cursor.execute('SELECT CodProv, CodFaena, CodLabor, IDNumCto, Temporada, CostoLabor, CodTipoPlanta, '
                          'FechaInicio, Unidad, CodVariedad, Vigente, FechaTermino, UnicoFundo, CodCuartel '
                          'FROM detallecontratista')
    registrosorigen = campos_cursor.rowcount
    print("(29) tabla detallecontratista")
    print(registrosorigen)
    for CodProv, CodFaena, CodLabor, IDNumCto, Temporada, CostoLabor, CodTipoPlanta, FechaInicio, Unidad, CodVariedad, Vigente, FechaTermino, UnicoFundo, CodCuartel in campos_cursor.fetchall():
        i = i + 1
        print(CodProv, CodFaena, CodLabor, IDNumCto, Temporada, CostoLabor, CodTipoPlanta, FechaInicio, Unidad,
              CodVariedad, Vigente, FechaTermino, UnicoFundo, CodCuartel)
        sql = "INSERT INTO " + EsquemaBD + ".detallecontratista(CodProv, CodFaena, CodLabor, IDNumCto, Temporada, " \
                                           "CostoLabor, CodTipoPlanta, FechaInicio, Unidad, CodVariedad, " \
                                           "Vigente, FechaTermino, UnicoFundo, CodCuartel) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (
        CodProv, CodFaena, CodLabor, IDNumCto, Temporada, CostoLabor, CodTipoPlanta, FechaInicio, Unidad, CodVariedad,
        Vigente, FechaTermino, UnicoFundo, CodCuartel)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallecontratista: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallecontratista', 'detallecontratista', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallefactcompra
    i = 0
    campos_cursor.execute('SELECT `CodigoFC`, `Cantidad`, `Codigo`, `Valor`, `SubTotalFact`, `Describe`, '
                          '`Tipo`, `NumODC`, `ValorSinDcto`, `ValorMoneda`, `CodCtaCtble`, `CodCC`, `IDLote`, '
                          '`IDDetFactCpra`, `Item`, `ConDetalle`, `CodServicio`, `NumGuia`, `ValorNota`, '
                          '`SubTotalNota`, `ValorCompra`, `DistibuyeIENR`, `PrecioKardex`, `NLineaODC`, '
                          '`CodFaena`, `Unidad` FROM `DetalleFactCompra`')
    registrosorigen = campos_cursor.rowcount
    print("(30) tabla detallefactcompra")
    print(registrosorigen)
    for CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Descripcion, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, IDLote, IDDetFactCpra, Item, ConDetalle, CodServicio, NumGuia, ValorNota, SubTotalNota, ValorCompra, DistibuyeIENR, PrecioKardex, NLineaODC, CodFaena, Unidad in campos_cursor.fetchall():
        i = i + 1
        print(CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Descripcion, Tipo, NumODC, ValorSinDcto, ValorMoneda,
              CodCtaCtble, CodCC, IDLote, IDDetFactCpra, Item, ConDetalle, CodServicio, NumGuia, ValorNota,
              SubTotalNota, ValorCompra, DistibuyeIENR, PrecioKardex, NLineaODC, CodFaena, Unidad)
        sql = "INSERT INTO " + EsquemaBD + ".detallefactcompra(CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, " \
                                           "Descripcion, Tipo, NumODC, ValorSinDcto, ValorMoneda, CodCtaCtble, CodCC, " \
                                           "IDLote, IDDetFactCpra, Item, ConDetalle, CodServicio, NumGuia, ValorNota, " \
                                           "SubTotalNota, ValorCompra, DistibuyeIENR, PrecioKardex, NLineaODC, CodFaena, Unidad) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodigoFC, Cantidad, Codigo, Valor, SubTotalFact, Descripcion, Tipo, NumODC, ValorSinDcto, ValorMoneda,
               CodCtaCtble, CodCC, IDLote, IDDetFactCpra, Item, ConDetalle, CodServicio, NumGuia, ValorNota,
               SubTotalNota, ValorCompra, DistibuyeIENR, PrecioKardex, NLineaODC, CodFaena, Unidad)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallefactcompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallefactcompra', 'detallefactcompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    campos_cursor.close()
    Campos.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas en Campos 1/9")



if __name__ == "__main__":
    main()
