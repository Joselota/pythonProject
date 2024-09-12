import pyodbc
import pymysql
import time
import datetime
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
    # VariablesGlobales
    EsquemaBD = "stagecampos"
    SistemaOrigen = "Campos"
    fechacarga = datetime.datetime.now()

    # Generando identificador para proceso de cuadratura
    dia = str(100+int(format(fechacarga.day)))
    mes = str(100+int(format(fechacarga.month)))
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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".dias")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallesolicitud")
    #bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".DetalleTratosTarifa")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Empresa")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Encargado")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".ExistenciaBodega")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FacturaCompra")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FaenaLabMaq")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FaenaLabor")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Faenas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Familias")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".File36")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FormaPagoPersonal")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FormatoLiqui")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Fundos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".GuiaFactCpra")

    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()

    #SELECT CodDia, NomDia FROM dias;
    #TABLA bodegas
    i = 0
    campos_cursor.execute('SELECT CodDia, NomDia FROM dias')
    registrosorigen = campos_cursor.rowcount
    print("(31) tabla dias")
    print(registrosorigen)
    for CodDia, NomDia in campos_cursor.fetchall():
        i = i + 1
        print(CodDia, NomDia)
        sql = "INSERT INTO " + EsquemaBD + ".dias(CodDia, NomDia) VALUES (%s, %s)"
        val = (CodDia, NomDia)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla dias: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'dias', 'dias', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    #SELECT ID, Logo FROM dialogos;
    #TABLA bodegas
    #i = 0
    #campos_cursor.execute('SELECT ID, Logo FROM dialogos')
    #registrosorigen = campos_cursor.rowcount
    #print("(1) tabla dialogos")
    #print(registrosorigen)
    #for ID, Logo in campos_cursor.fetchall():
    #    i = i + 1
    #    print(ID, Logo)
    #    sql = "INSERT INTO " + EsquemaBD + ".dialogos(ID, Logo) VALUES (%s, %s)"
    #    val = (ID, Logo)
    #    bdg_cursor.execute(sql, val)
    #    bdg.commit()
    #print("Cantidad de registros en la tabla dialogos: ", i)

    #SELECT `FpCodigo`, `FpDescribe`, `FpUsaCtaBanco` FROM `FormaPagoPersonal`
    #TABLA FormaPagoPersonal
    i = 0
    campos_cursor.execute('SELECT FpCodigo, FpDescribe, FpUsaCtaBanco FROM FormaPagoPersonal')
    registrosorigen = campos_cursor.rowcount
    print("(32) tabla FormaPagoPersonal")
    print(registrosorigen)
    for FpCodigo, FpDescribe, FpUsaCtaBanco in campos_cursor.fetchall():
        i = i + 1
        print(type(FpDescribe))
        print(FpCodigo, FpDescribe, FpUsaCtaBanco)
        sql = "INSERT INTO " + EsquemaBD + ".FormaPagoPersonal(FpCodigo, FpDescribe, FpUsaCtaBanco) " \
                                            "VALUES (%s, %s, %s)"
        val = (FpCodigo, FpDescribe, FpUsaCtaBanco)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla FormaPagoPersonal: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'FormaPagoPersonal', 'FormaPagoPersonal', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA detallesolicitud
    i = 0
    campos_cursor.execute('SELECT `NumSol`, `Cantidad`, `Describe`, `Valor`, `Sub_Total`, '
                          '`Tipo`, `Codigo`, `Stock`, `Unidad`, `Requerida`, `Saldo`, '
                          '`NFichaExp`, `tmpFecODC`, `tmpFecEntrega`, `tmpNumODC`, `tmpCantRec`, '
                          '`tmpRutProve`, `tmpFecRec`, `tmpCantODC`, `NLinea`, `Observacion`, '
                          '`CodMotivoSol`, `TieneODC`, `CodServicio` FROM `DetalleSolicitud`')
    registrosorigen = campos_cursor.rowcount
    print("(33) tabla detallesolicitud")
    print(registrosorigen)
    for NumSol, Cantidad, Descripcion, Valor, Sub_Total, Tipo, Codigo, Stock, Unidad, Requerida, Saldo, NFichaExp, tmpFecODC, tmpFecEntrega, tmpNumODC, tmpCantRec, tmpRutProve, tmpFecRec, tmpCantODC, NLinea, Observacion, CodMotivoSol, TieneODC, CodServicio in campos_cursor.fetchall():
        i = i + 1
        print(NumSol, Cantidad, Descripcion, Valor, Sub_Total, Tipo, Codigo, Stock, Unidad, Requerida, Saldo, NFichaExp, tmpFecODC, tmpFecEntrega, tmpNumODC, tmpCantRec, tmpRutProve, tmpFecRec, tmpCantODC, NLinea, Observacion, CodMotivoSol, TieneODC, CodServicio)
        sql = "INSERT INTO " + EsquemaBD + ".detallesolicitud(NumSol, Cantidad, Descripcion, Valor, " \
                                           "Sub_Total, Tipo, Codigo, Stock, Unidad, Requerida, Saldo, " \
                                           "NFichaExp, tmpFecODC, tmpFecEntrega, tmpNumODC, tmpCantRec, " \
                                           "tmpRutProve, tmpFecRec, tmpCantODC, NLinea, Observacion, " \
                                           "CodMotivoSol, TieneODC, CodServicio) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumSol, Cantidad, Descripcion, Valor, Sub_Total, Tipo, Codigo, Stock, Unidad, Requerida, Saldo, NFichaExp, tmpFecODC, tmpFecEntrega, tmpNumODC, tmpCantRec, tmpRutProve, tmpFecRec, tmpCantODC, NLinea, Observacion, CodMotivoSol, TieneODC, CodServicio)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla detallesolicitud: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallesolicitud', 'detallesolicitud', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # TABLA DetalleTratosTarifa
    i = 0
    campos_cursor.execute('SELECT `CodMes`, `CodPer`, `CodLabor`, `Tarifa`, `Dias`, `Folio`, '
                          '`CodPeriodo` FROM DetalleTratosTarifa')
    registrosorigen = campos_cursor.rowcount
    print("(34) tabla DetalleTratosTarifa")
    print(registrosorigen)
    for CodMes, CodPer, CodLabor, Tarifa, Dias, Folio, CodPeriodo in campos_cursor.fetchall():
        i = i + 1
        print(CodMes, CodPer, CodLabor, Tarifa, Dias, Folio, CodPeriodo)
        sql = "INSERT INTO " + EsquemaBD + ".DetalleTratosTarifa(CodMes, CodPer, CodLabor, Tarifa, " \
                                           "Dias, Folio, CodPeriodo) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (CodMes, CodPer, CodLabor, Tarifa, Dias, Folio, CodPeriodo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla DetalleTratosTarifa: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'DetalleTratosTarifa', 'DetalleTratosTarifa', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    #SELECT `CodEmpresa`, `NomEmpresa`, `HectEmpresa`, `XMAPAP_` FROM `Empresa`
    # TABLA DetalleTratosTarifa
    i = 0
    campos_cursor.execute('SELECT `CodEmpresa`, `NomEmpresa`, `HectEmpresa`, `XMAPAP_` FROM `Empresa`')
    registrosorigen = campos_cursor.rowcount
    print("(35) tabla Empresa")
    print(registrosorigen)
    for CodEmpresa, NomEmpresa, HectEmpresa, XMAPAP in campos_cursor.fetchall():
        i = i + 1
        print(CodEmpresa, NomEmpresa, HectEmpresa, XMAPAP)
        sql = "INSERT INTO " + EsquemaBD + ".Empresa(CodEmpresa, NomEmpresa, HectEmpresa, XMAPAP_) " \
                                            "VALUES (%s, %s, %s, %s)"
        val = (CodEmpresa, NomEmpresa, HectEmpresa, XMAPAP)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Empresa: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Empresa', 'Empresa', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # TABLA Encargado
    i = 0
    campos_cursor.execute('SELECT `CodEnc`, `NomEnc`, `CorreoEnc`, `CodFundo`, `Dominio`, '
                          '`ClaveCorreo` FROM `Encargado`')
    registrosorigen = campos_cursor.rowcount
    print("(36) tabla Encargado")
    print(registrosorigen)
    for CodEnc, NomEnc, CorreoEnc, CodFundo, Dominio, ClaveCorreo in campos_cursor.fetchall():
        i = i + 1
        print(CodEnc, NomEnc, CorreoEnc, CodFundo, Dominio, ClaveCorreo)
        sql = "INSERT INTO " + EsquemaBD + ".Encargado(CodEnc, NomEnc, CorreoEnc, CodFundo, Dominio, ClaveCorreo) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (CodEnc, NomEnc, CorreoEnc, CodFundo, Dominio, ClaveCorreo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Encargado: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Encargado', 'Encargado', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # TABLA ExistenciaBodega
    i = 0
    campos_cursor.execute('SELECT `CodProd`, `CodImple`, `BdgCodigo`, `Existencia`, `NumExportacion`, '
                          '`Fisico`, `Diferencia`, `Inventario` FROM `ExistenciaBodega`')
    registrosorigen = campos_cursor.rowcount
    print("(37) tabla ExistenciaBodega")
    print(registrosorigen)
    for CodProd, CodImple, BdgCodigo, Existencia, NumExportacion, Fisico, Diferencia, Inventario in campos_cursor.fetchall():
        i = i + 1
        print(CodProd, CodImple, BdgCodigo, Existencia, NumExportacion, Fisico, Diferencia, Inventario)
        sql = "INSERT INTO " + EsquemaBD + ".ExistenciaBodega(CodProd, CodImple, BdgCodigo, Existencia, " \
                                           "NumExportacion, Fisico, Diferencia, Inventario) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodProd, CodImple, BdgCodigo, Existencia, NumExportacion, Fisico, Diferencia, Inventario)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla ExistenciaBodega: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ExistenciaBodega', 'ExistenciaBodega', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # TABLA FacturaCompra
    i = 0
    campos_cursor.execute('SELECT `NumFC`, `FechaFC`, `CodProv`, `CodigoFC`, `NetoFC`, `IvaFC`, `TotalFC`,'
                          ' `NumODC`, `CodBod`, `GlosaFC`, `TipoDoc`, `CodMon`, `TasaFC`, `DescuentoFC`,'
                          '`RebajaExistenciaFC`, `ExentaFC`, `AbonosFC`, `SaldoFC`, `EstadoFC`, '
                          '`FechaVenceFC`, `CodOper`, `CodCC`, `FechaContabilizaFC`, `MontoImpEspNoRecFC`, '
                          '`OtrosImpuestosFC`, `NumPeriodo`, `MueveExistencia`, `Desde_Campos`, '
                          '`MontoImpEspRecFC`, `MixtaFC`, `NumExportacion`, `NumFolio`, `NumSolEgreso`, '
                          '`PorcDescuentoFC`, `ConCuarteles`, `FechaIngreso`, `FueContabilizada`, '
                          '`ConNota`, `NetoFCNota`, `IvaFCNota`, `TotalFCNota`, `MontoAfectoFC`, '
                          '`MontoExentoFC`, `EsTercero`, `Retencion`, `OperacionCtbleFC`, `ExentoFCNota`, '
                          '`AfectoFCNota`, `FechaRecepcionFC`, `DiferenciaPrecio`, `CentralizadaCons`, '
                          '`DigitadaPor`, `EsContratista`, `CodCondPago` FROM `FacturaCompra`')
    registrosorigen = campos_cursor.rowcount
    print("(38) tabla FacturaCompra")
    print(registrosorigen)
    for NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, RebajaExistenciaFC, ExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoImpEspNoRecFC, OtrosImpuestosFC, NumPeriodo, MueveExistencia, Desde_Campos, MontoImpEspRecFC, MixtaFC, NumExportacion, NumFolio, NumSolEgreso, PorcDescuentoFC, ConCuarteles, FechaIngreso, FueContabilizada, ConNota, NetoFCNota, IvaFCNota, TotalFCNota, MontoAfectoFC, MontoExentoFC, EsTercero, Retencion, OperacionCtbleFC, ExentoFCNota, AfectoFCNota, FechaRecepcionFC, DiferenciaPrecio, CentralizadaCons, DigitadaPor, EsContratista, CodCondPago in campos_cursor.fetchall():
        i = i + 1
        print(NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, RebajaExistenciaFC, ExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoImpEspNoRecFC, OtrosImpuestosFC, NumPeriodo, MueveExistencia, Desde_Campos, MontoImpEspRecFC, MixtaFC, NumExportacion, NumFolio, NumSolEgreso, PorcDescuentoFC, ConCuarteles, FechaIngreso, FueContabilizada, ConNota, NetoFCNota, IvaFCNota, TotalFCNota, MontoAfectoFC, MontoExentoFC, EsTercero, Retencion, OperacionCtbleFC, ExentoFCNota, AfectoFCNota, FechaRecepcionFC, DiferenciaPrecio, CentralizadaCons, DigitadaPor, EsContratista, CodCondPago)
        sql = "INSERT INTO " + EsquemaBD + ".FacturaCompra(NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, " \
                                           "TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, " \
                                           "RebajaExistenciaFC, ExentaFC, AbonosFC, SaldoFC, EstadoFC, " \
                                           "FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoImpEspNoRecFC, " \
                                           "OtrosImpuestosFC, NumPeriodo, MueveExistencia, Desde_Campos," \
                                           " MontoImpEspRecFC, MixtaFC, NumExportacion, NumFolio, NumSolEgreso," \
                                           " PorcDescuentoFC, ConCuarteles, FechaIngreso, FueContabilizada, " \
                                           "ConNota, NetoFCNota, IvaFCNota, TotalFCNota, MontoAfectoFC, " \
                                           "MontoExentoFC, EsTercero, Retencion, OperacionCtbleFC, ExentoFCNota, " \
                                           "AfectoFCNota, FechaRecepcionFC, DiferenciaPrecio, CentralizadaCons, " \
                                           "DigitadaPor, EsContratista, CodCondPago) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumFC, FechaFC, CodProv, CodigoFC, NetoFC, IvaFC, TotalFC, NumODC, CodBod, GlosaFC, TipoDoc, CodMon, TasaFC, DescuentoFC, RebajaExistenciaFC, ExentaFC, AbonosFC, SaldoFC, EstadoFC, FechaVenceFC, CodOper, CodCC, FechaContabilizaFC, MontoImpEspNoRecFC, OtrosImpuestosFC, NumPeriodo, MueveExistencia, Desde_Campos, MontoImpEspRecFC, MixtaFC, NumExportacion, NumFolio, NumSolEgreso, PorcDescuentoFC, ConCuarteles, FechaIngreso, FueContabilizada, ConNota, NetoFCNota, IvaFCNota, TotalFCNota, MontoAfectoFC, MontoExentoFC, EsTercero, Retencion, OperacionCtbleFC, ExentoFCNota, AfectoFCNota, FechaRecepcionFC, DiferenciaPrecio, CentralizadaCons, DigitadaPor, EsContratista, CodCondPago)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla FacturaCompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'FacturaCompra', 'FacturaCompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # TABLA FaenaLabMaq
    i = 0
    campos_cursor.execute('SELECT CodLabMaq, CodFaena FROM FaenaLabMaq')
    registrosorigen = campos_cursor.rowcount
    print("(39) tabla FaenaLabMaq")
    print(registrosorigen)
    for CodLabMaq, CodFaena in campos_cursor.fetchall():
        i = i + 1
        print(CodLabMaq, CodFaena)
        sql = "INSERT INTO " + EsquemaBD + ".FaenaLabMaq(CodLabMaq, CodFaena) " \
                                            "VALUES (%s, %s)"
        val = (CodLabMaq, CodFaena)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla FaenaLabMaq: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'FaenaLabMaq', 'FaenaLabMaq', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # TABLA FaenaLabor
    i = 0
    campos_cursor.execute('SELECT CodLabor, CodFaena FROM FaenaLabor')
    registrosorigen = campos_cursor.rowcount
    print("(40) tabla FaenaLabor")
    print(registrosorigen)
    for CodLabor, CodFaena in campos_cursor.fetchall():
        i = i + 1
        print(CodLabor, CodFaena)
        sql = "INSERT INTO " + EsquemaBD + ".FaenaLabor(CodLabor, CodFaena) " \
                                            "VALUES (%s, %s)"
        val = (CodLabor, CodFaena)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla FaenaLabor: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'FaenaLabor', 'FaenaLabor', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # TABLA Faenas
    i = 0
    campos_cursor.execute('SELECT CodFaena, NomFaena FROM Faenas')
    registrosorigen = campos_cursor.rowcount
    print("(41) tabla Faenas")
    print(registrosorigen)
    for CodFaena, NomFaena in campos_cursor.fetchall():
        i = i + 1
        print(CodLabMaq, CodFaena)
        sql = "INSERT INTO " + EsquemaBD + ".Faenas(CodFaena, NomFaena) " \
                                            "VALUES (%s, %s)"
        val = (CodFaena, NomFaena)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Faenas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Faenas', 'Faenas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # TABLA Familias
    i = 0
    campos_cursor.execute('SELECT CodFam, NomFam, TipoFam, CodCtaCtble, TotalFam FROM Familias')
    registrosorigen = campos_cursor.rowcount
    print("(42) tabla Familias")
    print(registrosorigen)
    for CodFam, NomFam, TipoFam, CodCtaCtble, TotalFam in campos_cursor.fetchall():
        i = i + 1
        print(CodFam, NomFam, TipoFam, CodCtaCtble, TotalFam)
        sql = "INSERT INTO " + EsquemaBD + ".Familias(CodFam, NomFam, TipoFam, CodCtaCtble, TotalFam) " \
                                            "VALUES (%s, %s, %s, %s, %s)"
        val = (CodFam, NomFam, TipoFam, CodCtaCtble, TotalFam)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Familias: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Familias', 'Familias', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # TABLA File36
    i = 0
    campos_cursor.execute('SELECT MesFeriado, FechaFeriado, AgnoFeriado, DiaFeriado, FechaIngreso FROM File36')
    registrosorigen = campos_cursor.rowcount
    print("(43) tabla File36")
    print(registrosorigen)
    for MesFeriado, FechaFeriado, AgnoFeriado, DiaFeriado, FechaIngreso in campos_cursor.fetchall():
        i = i + 1
        print(MesFeriado, FechaFeriado, AgnoFeriado, DiaFeriado, FechaIngreso)
        sql = "INSERT INTO " + EsquemaBD + ".File36(MesFeriado, FechaFeriado, AgnoFeriado, " \
                                           "DiaFeriado, FechaIngreso) " \
                                            "VALUES (%s, %s, %s, %s, %s)"
        val = (MesFeriado, FechaFeriado, AgnoFeriado, DiaFeriado, FechaIngreso)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla File36: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'File36', 'File36', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    #TABLA FormatoLiqui
    i = 0
    campos_cursor.execute('SELECT CodPer, Bonos, Haberes, HorasExtras, Tratos, DesglosePrest, '
                          'DesgloseDcto, DesgloseLab, Colacion, Movilizacion FROM FormatoLiqui')
    registrosorigen = campos_cursor.rowcount
    print("(44) tabla FormatoLiqui")
    print(registrosorigen)
    for CodPer, Bonos, Haberes, HorasExtras, Tratos, DesglosePrest, DesgloseDcto, DesgloseLab, Colacion, Movilizacion in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, Bonos, Haberes, HorasExtras, Tratos, DesglosePrest, DesgloseDcto, DesgloseLab, Colacion, Movilizacion)
        sql = "INSERT INTO " + EsquemaBD + ".FormatoLiqui(CodPer, Bonos, Haberes, HorasExtras, Tratos, " \
                                           "DesglosePrest, DesgloseDcto, DesgloseLab, Colacion, Movilizacion) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodPer, Bonos, Haberes, HorasExtras, Tratos, DesglosePrest, DesgloseDcto, DesgloseLab, Colacion, Movilizacion)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla FormatoLiqui: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'FormatoLiqui', 'FormatoLiqui', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    #TABLA Fundos
    i = 0
    campos_cursor.execute('SELECT CodFundo, NomFundo, CodPredio, HectFundo, RazonFundo, UnicoFundo, '
                          'DomFundo, CiuFundo, RutFundo, NoProductivo, CodCC, LugarTrabajoRem700, '
                          'DiasAdicVac, HoraInicioAM, HoraTerminoAM, HoraInicioPM, HoraTerminoPM, '
                          'HoraInicioAMSab, HoraTerminoAMSab, HoraInicioPMSab, HoraTerminoPMSab, '
                          'FolioLibroAsist FROM Fundos')
    registrosorigen = campos_cursor.rowcount
    print("(45) tabla Fundos")
    print(registrosorigen)
    for CodFundo, NomFundo, CodPredio, HectFundo, RazonFundo, UnicoFundo, DomFundo, CiuFundo, RutFundo, NoProductivo, CodCC, LugarTrabajoRem700, DiasAdicVac, HoraInicioAM, HoraTerminoAM, HoraInicioPM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoAMSab, HoraInicioPMSab, HoraTerminoPMSab, FolioLibroAsist in campos_cursor.fetchall():
        i = i + 1
        print(CodFundo, NomFundo, CodPredio, HectFundo, RazonFundo, UnicoFundo, DomFundo, CiuFundo, RutFundo, NoProductivo, CodCC, LugarTrabajoRem700, DiasAdicVac, HoraInicioAM, HoraTerminoAM, HoraInicioPM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoAMSab, HoraInicioPMSab, HoraTerminoPMSab, FolioLibroAsist)
        sql = "INSERT INTO " + EsquemaBD + ".Fundos(CodFundo, NomFundo, CodPredio, HectFundo, RazonFundo, " \
                                           "UnicoFundo, DomFundo, CiuFundo, RutFundo, NoProductivo, CodCC, " \
                                           "LugarTrabajoRem700, DiasAdicVac, HoraInicioAM, HoraTerminoAM, " \
                                           "HoraInicioPM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoAMSab, " \
                                           "HoraInicioPMSab, HoraTerminoPMSab, FolioLibroAsist) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodFundo, NomFundo, CodPredio, HectFundo, RazonFundo, UnicoFundo, DomFundo, CiuFundo, RutFundo, NoProductivo, CodCC, LugarTrabajoRem700, DiasAdicVac, HoraInicioAM, HoraTerminoAM, HoraInicioPM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoAMSab, HoraInicioPMSab, HoraTerminoPMSab, FolioLibroAsist)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Fundos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Fundos', 'Fundos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    #TABLA GuiaFactCpra
    i = 0
    campos_cursor.execute('SELECT CodigoFC, FechaGuia, CodCom, NumFC, NumGuia, Desvincular FROM GuiaFactCpra')
    registrosorigen = campos_cursor.rowcount
    print("(46) tabla GuiaFactCpra")
    print(registrosorigen)
    for CodigoFC, FechaGuia, CodCom, NumFC, NumGuia, Desvincular in campos_cursor.fetchall():
        i = i + 1
        if FechaGuia == None:
            FechaGuia2 = None
        else:
            FechaGuia2 = FechaGuia

        print(CodigoFC, FechaGuia2, CodCom, NumFC, NumGuia, Desvincular)
        sql = "INSERT INTO " + EsquemaBD + ".GuiaFactCpra(CodigoFC, FechaGuia, CodCom, NumFC, NumGuia, Desvincular) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (CodigoFC, FechaGuia2, CodCom, NumFC, NumGuia, Desvincular)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla GuiaFactCpra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'GuiaFactCpra', 'GuiaFactCpra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    campos_cursor.close()
    Campos.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas en Campos 2/9")



if __name__ == "__main__":
    main()