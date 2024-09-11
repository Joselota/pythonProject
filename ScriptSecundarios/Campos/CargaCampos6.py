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

    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OrdenCompra")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Parametros")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Periodo")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Personal")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".PlanCuentas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".PptoDetalleCC")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".PptoItems")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".PptoItemsDetalle")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Prioridad")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Produccion")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Productos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Proveedores")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".RangoCargas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".RecepcionServicios")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Sector")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Servicios")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".SolicitudODC")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".SubFamilias")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Temporadas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TipoCargas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TipoContratos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TipoDocumentos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TipoPlantas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TiposParametros")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".TramosImpuesto")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".ValoresTiposParametros")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".ValoresUF")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".ValoresUTM")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Variedades")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()


    # Tabla `TiposParametros`
    i = 0
    campos_cursor.execute('SELECT `CodigoTP`, `NombreTP`, `DescribeTP` FROM TiposParametros')
    registrosorigen = campos_cursor.rowcount
    print("(72) tabla TiposParametros")
    print(registrosorigen)
    for CodigoTP, NombreTP, DescribeTP in campos_cursor.fetchall():
        i = i + 1
        print(CodigoTP, NombreTP, DescribeTP)
        sql = "INSERT INTO " + EsquemaBD + ".TiposParametros (CodigoTP, NombreTP, DescribeTP) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodigoTP, NombreTP, DescribeTP)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TiposParametros: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TiposParametros', 'TiposParametros', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla TipoContratos
    i = 0
    campos_cursor.execute('SELECT `TcCodigo`, `TcDescribe`, `TcPagoDia`, `TcConHaberes`, `TcConDctos`, `TcConFeriados`, '
                          '`TcConSemCorr`, `TcConAFP`, `TcConIsapre`, `TcCodRem`, `TcTipoPlazo`, `TcPagoMes`, '
                          '`TcPagoFaena`, `TcSegCesantiaDctoTrab` FROM TipoContratos')
    registrosorigen = campos_cursor.rowcount
    print("(73) tabla TipoContratos")
    print(registrosorigen)
    for TcCodigo, TcDescribe, TcPagoDia, TcConHaberes, TcConDctos, TcConFeriados, TcConSemCorr, TcConAFP, TcConIsapre, TcCodRem, TcTipoPlazo, TcPagoMes, TcPagoFaena, TcSegCesantiaDctoTrab in campos_cursor.fetchall():
        i = i + 1
        print(TcCodigo, TcDescribe, TcPagoDia, TcConHaberes, TcConDctos, TcConFeriados, TcConSemCorr, TcConAFP, TcConIsapre, TcCodRem, TcTipoPlazo, TcPagoMes, TcPagoFaena, TcSegCesantiaDctoTrab)
        sql = "INSERT INTO " + EsquemaBD + ".TipoContratos (TcCodigo, TcDescribe, TcPagoDia, TcConHaberes, TcConDctos, " \
                                           "TcConFeriados, TcConSemCorr, TcConAFP, TcConIsapre, TcCodRem, TcTipoPlazo, " \
                                           "TcPagoMes, TcPagoFaena, TcSegCesantiaDctoTrab) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (TcCodigo, TcDescribe, TcPagoDia, TcConHaberes, TcConDctos, TcConFeriados, TcConSemCorr, TcConAFP, TcConIsapre, TcCodRem, TcTipoPlazo, TcPagoMes, TcPagoFaena, TcSegCesantiaDctoTrab)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TipoContratos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TipoContratos', 'TipoContratos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `Temporadas`
    i = 0
    campos_cursor.execute('SELECT `Temporada`, `FechaInicio`, `FechaTermino`, `Campo`, `Observaciones`, `CodEmpresa` '
                          'FROM Temporadas')
    registrosorigen = campos_cursor.rowcount
    print("(74) tabla Temporadas")
    print(registrosorigen)
    for Temporada, FechaInicio, FechaTermino, Campo, Observaciones, CodEmpresa in campos_cursor.fetchall():
        i = i + 1
        print(Temporada, FechaInicio, FechaTermino, Campo, Observaciones, CodEmpresa)
        sql = "INSERT INTO " + EsquemaBD + ".Temporadas (Temporada, FechaInicio, FechaTermino, " \
                                           "Campo, Observaciones, CodEmpresa) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (Temporada, FechaInicio, FechaTermino, Campo, Observaciones, CodEmpresa)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Temporadas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Temporadas', 'Temporadas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla SolicitudODC
    i = 0
    campos_cursor.execute('SELECT `NumSol`, `Fecha`, `Entrega`, `Nota`, `Neto`, `Iva`, `Total`, `Estado`, `Solicita`, '
                          '`Aprueba`, `CodPrioridad`, `ODC`, `CodCC`, `CodProv`, `NomProv`, `Lugar`, `Atencion`, '
                          '`NomSolicita`, `Exenta`, `ComentEstado`, `NomAprueba`, `Alternativas`, `Recibe`, `NomRecibe`, '
                          '`SinTraspaso`, `FechaAprb`, `Desde_Campos`, `BdgCodigo`, `Digita`, `NomDigita`, `NumSIC` '
                          'FROM SolicitudODC')
    registrosorigen = campos_cursor.rowcount
    print("(75) tabla SolicitudODC")
    print(registrosorigen)
    for NumSol, Fecha, Entrega, Nota, Neto, Iva, Total, Estado, Solicita, Aprueba, CodPrioridad, ODC, CodCC, CodProv, NomProv, Lugar, Atencion, NomSolicita, Exenta, ComentEstado, NomAprueba, Alternativas, Recibe, NomRecibe, SinTraspaso, FechaAprb, Desde_Campos, BdgCodigo, Digita, NomDigita, NumSIC in campos_cursor.fetchall():
        i = i + 1
        print(NumSol, Fecha, Entrega, Nota, Neto, Iva, Total, Estado, Solicita, Aprueba, CodPrioridad, ODC, CodCC, CodProv, NomProv, Lugar, Atencion, NomSolicita, Exenta, ComentEstado, NomAprueba, Alternativas, Recibe, NomRecibe, SinTraspaso, FechaAprb, Desde_Campos, BdgCodigo, Digita, NomDigita, NumSIC)
        sql = "INSERT INTO " + EsquemaBD + ".SolicitudODC (NumSol, Fecha, Entrega, Nota, Neto, Iva, Total, Estado, Solicita, " \
                                           "Aprueba, CodPrioridad, ODC, CodCC, CodProv, NomProv, Lugar, Atencion, NomSolicita, " \
                                           "Exenta, ComentEstado, NomAprueba, Alternativas, Recibe, NomRecibe, SinTraspaso, " \
                                           "FechaAprb, Desde_Campos, BdgCodigo, Digita, NomDigita, NumSIC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumSol, Fecha, Entrega, Nota, Neto, Iva, Total, Estado, Solicita, Aprueba, CodPrioridad, ODC, CodCC, CodProv, NomProv, Lugar, Atencion, NomSolicita, Exenta, ComentEstado, NomAprueba, Alternativas, Recibe, NomRecibe, SinTraspaso, FechaAprb, Desde_Campos, BdgCodigo, Digita, NomDigita, NumSIC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla SolicitudODC: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'SolicitudODC', 'SolicitudODC', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla RecepcionServicios
    i = 0
    campos_cursor.execute('SELECT `CodCom`, `Fecha`, `CodServ`, `Cantidad`, `Valor`, `ValorPesos`, `Sub_Total`, `NomSer`, '
                          '`NumODC`, `CodCC`, `Hora`, `BdgCodigo`, `CodCtaCtble`, `NumDocAso`, `TipoDocAso`, `Tipo`, '
                          '`Mes`, `IvaRec`, `IvaNoRec`, `NLineaODC`, `IDDetRecep`, `CantidadNC` FROM RecepcionServicios')
    registrosorigen = campos_cursor.rowcount
    print("(76) tabla RecepcionServicios")
    print(registrosorigen)
    for CodCom, Fecha, CodServ, Cantidad, Valor, ValorPesos, Sub_Total, NomSer, NumODC, CodCC, Hora, BdgCodigo, CodCtaCtble, NumDocAso, TipoDocAso, Tipo, Mes, IvaRec, IvaNoRec, NLineaODC, IDDetRecep, CantidadNC in campos_cursor.fetchall():
        i = i + 1
        print(CodCom, Fecha, CodServ, Cantidad, Valor, ValorPesos, Sub_Total, NomSer, NumODC, CodCC, Hora, BdgCodigo, CodCtaCtble, NumDocAso, TipoDocAso, Tipo, Mes, IvaRec, IvaNoRec, NLineaODC, IDDetRecep, CantidadNC)
        sql = "INSERT INTO " + EsquemaBD + ".RecepcionServicios (CodCom, Fecha, CodServ, Cantidad, Valor, ValorPesos, Sub_Total, " \
                                           "NomSer, NumODC, CodCC, Hora, BdgCodigo, CodCtaCtble, NumDocAso, TipoDocAso, Tipo, Mes, " \
                                           "IvaRec, IvaNoRec, NLineaODC, IDDetRecep, CantidadNC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s)"
        val = (CodCom, Fecha, CodServ, Cantidad, Valor, ValorPesos, Sub_Total, NomSer, NumODC, CodCC, Hora, BdgCodigo, CodCtaCtble, NumDocAso, TipoDocAso, Tipo, Mes, IvaRec, IvaNoRec, NLineaODC, IDDetRecep, CantidadNC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla RecepcionServicios: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'RecepcionServicios', 'RecepcionServicios', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()





    # Tabla `Proveedores`
    i = 0
    campos_cursor.execute('SELECT `CodProv`, `NomProv`, `Direccion`, `Giro`, `Fono`, `Fax`, `Ciudad`, `Region`, `Rubro`, '
                          '`Vendedor`, `EsContratista`, `EsConcesionario`, `LugarPago` FROM Proveedores')
    registrosorigen = campos_cursor.rowcount
    print("(77) tabla Proveedores")
    print(registrosorigen)
    for CodProv, NomProv, Direccion, Giro, Fono, Fax, Ciudad, Region, Rubro, Vendedor, EsContratista, EsConcesionario, LugarPago in campos_cursor.fetchall():
        i = i + 1
        print(CodProv, NomProv, Direccion, Giro, Fono, Fax, Ciudad, Region, Rubro, Vendedor, EsContratista, EsConcesionario, LugarPago)
        sql = "INSERT INTO " + EsquemaBD + ".Proveedores (CodProv, NomProv, Direccion, Giro, Fono, Fax, Ciudad, Region, Rubro, Vendedor, EsContratista, EsConcesionario, LugarPago) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodProv, NomProv, Direccion, Giro, Fono, Fax, Ciudad, Region, Rubro, Vendedor, EsContratista, EsConcesionario, LugarPago)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Proveedores: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Proveedores', 'Proveedores', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla Personal
    i = 0
    campos_cursor.execute('SELECT `CodPer`, `NomPer`, `DireccionPer`, `CodCargo`, `CodAfp`, `CodIsa`, `NumCargas`, `MinimoImponible`, '
                          '`Select`, `Cuadrilla`, `ValDiaPer`, `FechaNacPer`, `EstadoCivilPer`, `Contratado`, `TipoContrato`, '
                          '`FechaInicio`, `FechaTermino`, `Disponible`, `SueldoFijo`, `CiudadPer`, `Finiquitado`, `CodLabor`, '
                          '`UnicoFundo`, `Calificacion`, `MotivoFiniquito`, `CalcSemCorr`, `CalcFeriados`, `ValDiaMinimo`, '
                          '`ValDiaPer31`, `ValDiaPer28`, `TotDetalleFiniquito`, `TextDesgloseFiniquito`, `ConCesantia`, '
                          '`FonoContacto`, `CesantiaCargoEmp`, `PlanIsapreUF`, `FechaIngreso`, `Liquido_Bruto`, `PagoTratosImponible`, '
                          '`Nombres`, `ApellidoPaterno`, `ApellidoMaterno`, `Sexo`, `LugarNacimiento`, `Nacionalidad`, `eMail`, '
                          '`CodBanco`, `CodCuentaBanco`, `CategoriaRem700`, `EsContratista`, `CodContratista`, `Comentarios`, '
                          '`AlimentacionEjecutiva`, `LugarIntegro`, `FpCodigo`, `TcCodigo`, `CodCajaComp`, `CodMutual`, `Bloqueado`, '
                          '`PlanIsaprePesos`, `AvisoFiniquito`, `CodClasifica`, `LugarPagoRem700`, `JornadaRem700`, `Profesion`, '
                          '`Escolaridad`, `CalJuridicaRem700`, `Administrativo`, `DescribeFaena`, `FolioVigente`, `RangoCarga`, '
                          '`FechaFirmaFiniquito`, `Renuncia`, `RentaFIja`, `FiniquitoConLiquida`, `ValDiaPer29`, `SinGratificacion`, '
                          '`DiasSemanaPer`, `PagaSabado`, `ComoPagaSabado`, `HoraInicioAM`, `HoraTerminoPM`, `HoraInicioAMSab`, '
                          '`HoraTerminoPMSab`, `Jubilado`, `CodAFC`, `CodAPV`, `PesosAPV`, `UFAPV`, `PorcenAPV`, `InicioAPV`, '
                          '`TerminoAPV`, `PesosAV`, `UFAV`, `InicioAV`, `TerminoAV`, `PesosCVA`, `UFCVA`, `InicioCVA`, `TerminoCVA`, '
                          '`CodAV`, `CodCVA`, `Poliza`, `HoraTerminoAM`, `HoraInicioPM`, `HoraTerminoAMSab`, `HoraInicioPMSab`, `PorcAV`, '
                          '`PorcCVA`, `polizaAV`, `PolizaCVA`, `GrupoC`, `Division`, `SeguroC_11agnos` FROM Personal')
    registrosorigen = campos_cursor.rowcount
    print("(78) tabla Personal")
    print(registrosorigen)
    for CodPer, NomPer, DireccionPer, CodCargo, CodAfp, CodIsa, NumCargas, MinimoImponible, Select, Cuadrilla, ValDiaPer, FechaNacPer, EstadoCivilPer, Contratado, TipoContrato, FechaInicio, FechaTermino, Disponible, SueldoFijo, CiudadPer, Finiquitado, CodLabor, UnicoFundo, Calificacion, MotivoFiniquito, CalcSemCorr, CalcFeriados, ValDiaMinimo, ValDiaPer31, ValDiaPer28, TotDetalleFiniquito, TextDesgloseFiniquito, ConCesantia, FonoContacto, CesantiaCargoEmp, PlanIsapreUF, FechaIngreso, Liquido_Bruto, PagoTratosImponible, Nombres, ApellidoPaterno, ApellidoMaterno, Sexo, LugarNacimiento, Nacionalidad, eMail, CodBanco, CodCuentaBanco, CategoriaRem700, EsContratista, CodContratista, Comentarios, AlimentacionEjecutiva, LugarIntegro, FpCodigo, TcCodigo, CodCajaComp, CodMutual, Bloqueado, PlanIsaprePesos, AvisoFiniquito, CodClasifica, LugarPagoRem700, JornadaRem700, Profesion, Escolaridad, CalJuridicaRem700, Administrativo, DescribeFaena, FolioVigente, RangoCarga, FechaFirmaFiniquito, Renuncia, RentaFIja, FiniquitoConLiquida, ValDiaPer29, SinGratificacion, DiasSemanaPer, PagaSabado, ComoPagaSabado, HoraInicioAM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoPMSab, Jubilado, CodAFC, CodAPV, PesosAPV, UFAPV, PorcenAPV, InicioAPV, TerminoAPV, PesosAV, UFAV, InicioAV, TerminoAV, PesosCVA, UFCVA, InicioCVA, TerminoCVA, CodAV, CodCVA, Poliza, HoraTerminoAM, HoraInicioPM, HoraTerminoAMSab, HoraInicioPMSab, PorcAV, PorcCVA, polizaAV, PolizaCVA, GrupoC, Division, SeguroC_11agnos in campos_cursor.fetchall():
        i = i + 1
        xContrato = None
        xFiniquito = None
        print(CodPer, NomPer, DireccionPer, CodCargo, CodAfp, CodIsa, NumCargas, MinimoImponible, Select, Cuadrilla, ValDiaPer, FechaNacPer, EstadoCivilPer, Contratado, TipoContrato, FechaInicio, FechaTermino, Disponible, SueldoFijo, CiudadPer, Finiquitado, CodLabor, UnicoFundo, Calificacion, MotivoFiniquito, CalcSemCorr, CalcFeriados, ValDiaMinimo, ValDiaPer31, ValDiaPer28, TotDetalleFiniquito, TextDesgloseFiniquito, ConCesantia, FonoContacto, CesantiaCargoEmp, PlanIsapreUF, FechaIngreso, Liquido_Bruto, xContrato, xFiniquito, PagoTratosImponible, Nombres, ApellidoPaterno, ApellidoMaterno, Sexo, LugarNacimiento, Nacionalidad, eMail, CodBanco, CodCuentaBanco, CategoriaRem700, EsContratista, CodContratista, Comentarios, AlimentacionEjecutiva, LugarIntegro, FpCodigo, TcCodigo, CodCajaComp, CodMutual, Bloqueado, PlanIsaprePesos, AvisoFiniquito, CodClasifica, LugarPagoRem700, JornadaRem700, Profesion, Escolaridad, CalJuridicaRem700, Administrativo, DescribeFaena, FolioVigente, RangoCarga, FechaFirmaFiniquito, Renuncia, RentaFIja, FiniquitoConLiquida, ValDiaPer29, SinGratificacion, DiasSemanaPer, PagaSabado, ComoPagaSabado, HoraInicioAM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoPMSab, Jubilado, CodAFC, CodAPV, PesosAPV, UFAPV, PorcenAPV, InicioAPV, TerminoAPV, PesosAV, UFAV, InicioAV, TerminoAV, PesosCVA, UFCVA, InicioCVA, TerminoCVA, CodAV, CodCVA, Poliza, HoraTerminoAM, HoraInicioPM, HoraTerminoAMSab, HoraInicioPMSab, PorcAV, PorcCVA, polizaAV, PolizaCVA, GrupoC, Division, SeguroC_11agnos)
        sql = "INSERT INTO " + EsquemaBD + ".Personal (CodPer, NomPer, DireccionPer, CodCargo, CodAfp, CodIsa, NumCargas, " \
                                           "MinimoImponible, Seleccion, Cuadrilla, ValDiaPer, FechaNacPer, EstadoCivilPer," \
                                           " Contratado, TipoContrato, FechaInicio, FechaTermino, Disponible, SueldoFijo, " \
                                           "CiudadPer, Finiquitado, CodLabor, UnicoFundo, Calificacion, MotivoFiniquito, " \
                                           "CalcSemCorr, CalcFeriados, ValDiaMinimo, ValDiaPer31, ValDiaPer28, " \
                                           "TotDetalleFiniquito, " \
                             "TextDesgloseFiniquito, ConCesantia, FonoContacto, CesantiaCargoEmp, PlanIsapreUF," \
                            " FechaIngreso, Liquido_Bruto, xContrato, xFiniquito, PagoTratosImponible, Nombres, " \
                             "ApellidoPaterno, ApellidoMaterno, Sexo, LugarNacimiento, Nacionalidad, eMail, " \
                             "CodBanco, CodCuentaBanco, CategoriaRem700, EsContratista, CodContratista, Comentarios, " \
                            "AlimentacionEjecutiva, LugarIntegro, FpCodigo, TcCodigo, CodCajaComp, CodMutual, " \
                             "Bloqueado, PlanIsaprePesos, AvisoFiniquito, CodClasifica, LugarPagoRem700, JornadaRem700, " \
                            "Profesion, Escolaridad, CalJuridicaRem700, Administrativo, DescribeFaena, FolioVigente, " \
                            "RangoCarga, FechaFirmaFiniquito, Renuncia, RentaFIja, FiniquitoConLiquida, ValDiaPer29, " \
                            "SinGratificacion, DiasSemanaPer, PagaSabado, ComoPagaSabado, HoraInicioAM, HoraTerminoPM," \
                                " HoraInicioAMSab, HoraTerminoPMSab, Jubilado, CodAFC, CodAPV, PesosAPV, UFAPV, " \
                                 "PorcenAPV, InicioAPV, TerminoAPV, PesosAV, UFAV, InicioAV, TerminoAV, PesosCVA, UFCVA, " \
                                 "InicioCVA, TerminoCVA, CodAV, CodCVA, Poliza, HoraTerminoAM, HoraInicioPM, " \
                                 "HoraTerminoAMSab, HoraInicioPMSab, PorcAV, PorcCVA, polizaAV, PolizaCVA, GrupoC, " \
                                           "Division, SeguroC_11agnos) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s)"
        val = (CodPer, NomPer, DireccionPer, CodCargo, CodAfp, CodIsa, NumCargas, MinimoImponible, Select, Cuadrilla, ValDiaPer, FechaNacPer, EstadoCivilPer, Contratado, TipoContrato, FechaInicio, FechaTermino, Disponible, SueldoFijo, CiudadPer, Finiquitado, CodLabor, UnicoFundo, Calificacion, MotivoFiniquito, CalcSemCorr, CalcFeriados, ValDiaMinimo, ValDiaPer31, ValDiaPer28, TotDetalleFiniquito, TextDesgloseFiniquito, ConCesantia, FonoContacto, CesantiaCargoEmp, PlanIsapreUF, FechaIngreso, Liquido_Bruto, xContrato, xFiniquito, PagoTratosImponible, Nombres, ApellidoPaterno, ApellidoMaterno, Sexo, LugarNacimiento, Nacionalidad, eMail, CodBanco, CodCuentaBanco, CategoriaRem700, EsContratista, CodContratista, Comentarios, AlimentacionEjecutiva, LugarIntegro, FpCodigo, TcCodigo, CodCajaComp, CodMutual, Bloqueado, PlanIsaprePesos, AvisoFiniquito, CodClasifica, LugarPagoRem700, JornadaRem700, Profesion, Escolaridad, CalJuridicaRem700, Administrativo, DescribeFaena, FolioVigente, RangoCarga, FechaFirmaFiniquito, Renuncia, RentaFIja, FiniquitoConLiquida, ValDiaPer29, SinGratificacion, DiasSemanaPer, PagaSabado, ComoPagaSabado, HoraInicioAM, HoraTerminoPM, HoraInicioAMSab, HoraTerminoPMSab, Jubilado, CodAFC, CodAPV, PesosAPV, UFAPV, PorcenAPV, InicioAPV, TerminoAPV, PesosAV, UFAV, InicioAV, TerminoAV, PesosCVA, UFCVA, InicioCVA, TerminoCVA, CodAV, CodCVA, Poliza, HoraTerminoAM, HoraInicioPM, HoraTerminoAMSab, HoraInicioPMSab, PorcAV, PorcCVA, polizaAV, PolizaCVA, GrupoC, Division, SeguroC_11agnos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Personal: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Personal', 'Personal', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    #SELECT `NumODC`, `FechaODC`, `FechaEntrega`, `CodProv`, `Nota`, `Neto`, `Iva`, `Total`, `Digita`, `LugarEntrega`, `NomProv`, `CodCC`, `CodEnc`, `NotaProveedor`, `Exenta`, `CodMon`, `VB`, `Autoriza`, `Solicita`, `Descuento`, `Atencion`, `NomDigita`, `Autoriza2`, `NomAutoriza2`, `OtrosImpuestos`, `Estado`, `FechaSolic`, `Prioridad`, `NomEnc`, `NomVB`, `ComentEstado`, `Emitida`, `Alternativas`, `Recibe`, `NomRecibe`, `NumSolicitud`, `SinTraspaso`, `Cotizacion`, `FechaCotizacion`, `PorcDcto`, `CodEmp`, `CodCondPago`, `FichaExpo`, `ObservaSolic`, `FechaApbSol`, `FechaEstPago`, `Desde_Campos`, `CodPreApb`, `NomPreApb` FROM `OrdenCompra`
    i = 0
    campos_cursor.execute('SELECT `NumODC`, `FechaODC`, `FechaEntrega`, `CodProv`, `Nota`, `Neto`, `Iva`, `Total`, '
                          '`Digita`, `LugarEntrega`, `NomProv`, `CodCC`, `CodEnc`, `NotaProveedor`, `Exenta`, `CodMon`, '
                          '`VB`, `Autoriza`, `Solicita`, `Descuento`, `Atencion`, `NomDigita`, `Autoriza2`, `NomAutoriza2`,'
                          ' `OtrosImpuestos`, `Estado`, `FechaSolic`, `Prioridad`, `NomEnc`, `NomVB`, `ComentEstado`, '
                          '`Emitida`, `Alternativas`, `Recibe`, `NomRecibe`, `NumSolicitud`, `SinTraspaso`, `Cotizacion`, '
                          '`FechaCotizacion`, `PorcDcto`, `CodEmp`, `CodCondPago`, `FichaExpo`, `ObservaSolic`, '
                          '`FechaApbSol`, `FechaEstPago`, `Desde_Campos`, `CodPreApb`, `NomPreApb` FROM OrdenCompra')
    registrosorigen = campos_cursor.rowcount
    print("(79) tabla OrdenCompra")
    print(registrosorigen)
    for NumODC, FechaODC, FechaEntrega, CodProv, Nota, Neto, Iva, Total, Digita, LugarEntrega, NomProv, CodCC, CodEnc, NotaProveedor, Exenta, CodMon, VB, Autoriza, Solicita, Descuento, Atencion, NomDigita, Autoriza2, NomAutoriza2, OtrosImpuestos, Estado, FechaSolic, Prioridad, NomEnc, NomVB, ComentEstado, Emitida, Alternativas, Recibe, NomRecibe, NumSolicitud, SinTraspaso, Cotizacion, FechaCotizacion, PorcDcto, CodEmp, CodCondPago, FichaExpo, ObservaSolic, FechaApbSol, FechaEstPago, Desde_Campos, CodPreApb, NomPreApb in campos_cursor.fetchall():
        i = i + 1
        if FechaEntrega == None:
            FechaEntrega2 = None
        else:
            FechaEntrega2 = FechaEntrega
        print(NumODC, FechaODC, FechaEntrega2, CodProv, Nota, Neto, Iva, Total, Digita, LugarEntrega, NomProv, CodCC, CodEnc, NotaProveedor, Exenta, CodMon, VB, Autoriza, Solicita, Descuento, Atencion, NomDigita, Autoriza2, NomAutoriza2, OtrosImpuestos, Estado, FechaSolic, Prioridad, NomEnc, NomVB, ComentEstado, Emitida, Alternativas, Recibe, NomRecibe, NumSolicitud, SinTraspaso, Cotizacion, FechaCotizacion, PorcDcto, CodEmp, CodCondPago, FichaExpo, ObservaSolic, FechaApbSol, FechaEstPago, Desde_Campos, CodPreApb, NomPreApb)
        sql = "INSERT INTO " + EsquemaBD + ".OrdenCompra (NumODC, FechaODC, FechaEntrega, CodProv, Nota, Neto, Iva, Total, " \
                                           "Digita, LugarEntrega, NomProv, CodCC, CodEnc, NotaProveedor, Exenta, CodMon, " \
                                           "VB, Autoriza, Solicita, Descuento, Atencion, NomDigita, Autoriza2, NomAutoriza2," \
                                           " OtrosImpuestos, Estado, FechaSolic, Prioridad, NomEnc, NomVB, ComentEstado, " \
                                           "Emitida, Alternativas, Recibe, NomRecibe, NumSolicitud, SinTraspaso, Cotizacion," \
                                           " FechaCotizacion, PorcDcto, CodEmp, CodCondPago, FichaExpo, ObservaSolic, " \
                                           "FechaApbSol, FechaEstPago, Desde_Campos, CodPreApb, NomPreApb) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumODC, FechaODC, FechaEntrega2, CodProv, Nota, Neto, Iva, Total, Digita, LugarEntrega, NomProv, CodCC, CodEnc, NotaProveedor, Exenta, CodMon, VB, Autoriza, Solicita, Descuento, Atencion, NomDigita, Autoriza2, NomAutoriza2, OtrosImpuestos, Estado, FechaSolic, Prioridad, NomEnc, NomVB, ComentEstado, Emitida, Alternativas, Recibe, NomRecibe, NumSolicitud, SinTraspaso, Cotizacion, FechaCotizacion, PorcDcto, CodEmp, CodCondPago, FichaExpo, ObservaSolic, FechaApbSol, FechaEstPago, Desde_Campos, CodPreApb, NomPreApb)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla OrdenCompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'OrdenCompra', 'OrdenCompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla Parametros
    i = 0
    campos_cursor.execute('SELECT Gratificacion, TopeImponible, CodHEx1, CodHEx2, CodFamSer, CodFamRep, FactHEx1M, '
                          'FactHEx2M, TopeCesantia, FormLicenciasR700, CodLabTrato, CiudadEmp, DomicilioEmp, '
                          'RazonSocialEmp, CodSemCorr, RUTEmp, CodFeriado, CodVacacion, ValorIngresoMinimo, ValorIVA, '
                          'PrecisionSueldos, AporteIndefinido, AporteFijo, TopeGratificacion, CodColacion, CodMovilizacion,'
                          'TieneRem700, CodSabado, CodDomingo, OpeContabFin700, CodLicencia, CodCalMermaProd, '
                          'CtaFacturasPagar, CtaIvaCredito, CtaOtrosImpuestos, CodMoneda, BodegaPpal, CorrConsumos, '
                          'PorcRetencion, CtaRetencion, CtaHonoraPagar, PathServer, CorrLiqODC, IPServerKupay, '
                          'NombreMaquina, ConConexionKupay, DiasDeTrabajo, ClaveDesignerKupay, FonoEmp, FaxEmp,'
                          ' GiroEmp, RutRepLegal, PaisEmp, xLista, CorrelativoBaseDatos, Host, ToeMail, '
                          'EliminaDespliegeValores, EliminaModStockFactura, ODBCDataSource, ODBCName, ODBCPass, '
                          'ODBCDataSourceF700, ODBCNameF700, ODBCPassF700, PorUnaFin700, DivFin700, UniFin700, '
                          'IdTesoreriaFin700, IdContabFin700, IdLCoFin700, IdLVeFin700, CtaIESNoRecuperable, '
                          'CtaIESRecuperable, ProcedimientosGlobal, LeyendaOC, PagoPorHilera, PorcAnticipo, '
                          'MaximoHE, CodBonoSobreTpo, LeyendaFacturarA, LeyendaAnexa, CodEmp, ImpMembreteDocCpra, '
                          'MembreteODC, TDFacturaCpra, TDNotaCredito, TDNotaDebito, ValorHEContratista, TDIngreso, '
                          'TDEgreso, TDTraspaso, TDGuiaCompra, TDAplicacion, CodBonoTratos, PagoTratos, FormAusenciasR700, '
                          'PathODC, CalculoCargasFicha, ConPreAprobacion, DiasVacaciones, TipoFiniquito, TarjaMesB30, '
                          'IPServerCampos, NomMaquinaCampos, ClaveDesignerCampos, PathBPA, TopeHExMes, HEx2SinMaximos, '
                          'FactHEx1D, FactHEx2D, BonoMensual, FactHEx1D2, MesInv, AgnoInv, VersionFIN700, ODBCDsnR700, '
                          'ODBCNombreR700, ODBCClaveR700, CodEmpR700, TDGuiaDevolucion, CtaFacturasRecibir, '
                          'TDBoletaHonorario, CtaNotaPorRecibir, CtaResultado, TituloLiquidacion, FactCpraServMaqConDist, '
                          'TDAjusteInventario, ToleranciaDocum, LabCostoEmpR700, LabFiniquitoR700, FormDiasLiqR700, '
                          'FormSemCorrR700, TipoProyectoR700, DiasBaseBonoTratos, VersionREM700, GeneraArchivos, '
                          'RemArchivoPlano, ValidaCuartelAreaDpto FROM Parametros')
    registrosorigen = campos_cursor.rowcount
    print("(80) tabla Parametros")
    print(registrosorigen)
    for Gratificacion, TopeImponible, CodHEx1, CodHEx2, CodFamSer, CodFamRep, FactHEx1M, FactHEx2M, TopeCesantia, FormLicenciasR700, CodLabTrato, CiudadEmp, DomicilioEmp, RazonSocialEmp, CodSemCorr, RUTEmp, CodFeriado, CodVacacion, ValorIngresoMinimo, ValorIVA, PrecisionSueldos, AporteIndefinido, AporteFijo, TopeGratificacion, CodColacion, CodMovilizacion, TieneRem700, CodSabado, CodDomingo, OpeContabFin700, CodLicencia, CodCalMermaProd, CtaFacturasPagar, CtaIvaCredito, CtaOtrosImpuestos, CodMoneda, BodegaPpal, CorrConsumos, PorcRetencion, CtaRetencion, CtaHonoraPagar, PathServer, CorrLiqODC, IPServerKupay, NombreMaquina, ConConexionKupay, DiasDeTrabajo, ClaveDesignerKupay, FonoEmp, FaxEmp, GiroEmp, RutRepLegal, PaisEmp, xLista, CorrelativoBaseDatos, Host, ToeMail, EliminaDespliegeValores, EliminaModStockFactura, ODBCDataSource, ODBCName, ODBCPass, ODBCDataSourceF700, ODBCNameF700, ODBCPassF700, PorUnaFin700, DivFin700, UniFin700, IdTesoreriaFin700, IdContabFin700, IdLCoFin700, IdLVeFin700, CtaIESNoRecuperable, CtaIESRecuperable, ProcedimientosGlobal, LeyendaOC, PagoPorHilera, PorcAnticipo, MaximoHE, CodBonoSobreTpo, LeyendaFacturarA, LeyendaAnexa, CodEmp, ImpMembreteDocCpra, MembreteODC, TDFacturaCpra, TDNotaCredito, TDNotaDebito, ValorHEContratista, TDIngreso, TDEgreso, TDTraspaso, TDGuiaCompra, TDAplicacion, CodBonoTratos, PagoTratos, FormAusenciasR700, PathODC, CalculoCargasFicha, ConPreAprobacion, DiasVacaciones, TipoFiniquito, TarjaMesB30, IPServerCampos, NomMaquinaCampos, ClaveDesignerCampos, PathBPA, TopeHExMes, HEx2SinMaximos, FactHEx1D, FactHEx2D, BonoMensual, FactHEx1D2, MesInv, AgnoInv, VersionFIN700, ODBCDsnR700, ODBCNombreR700, ODBCClaveR700, CodEmpR700, TDGuiaDevolucion, CtaFacturasRecibir, TDBoletaHonorario, CtaNotaPorRecibir, CtaResultado, TituloLiquidacion, FactCpraServMaqConDist, TDAjusteInventario, ToleranciaDocum, LabCostoEmpR700, LabFiniquitoR700, FormDiasLiqR700, FormSemCorrR700, TipoProyectoR700, DiasBaseBonoTratos, VersionREM700, GeneraArchivos, RemArchivoPlano, ValidaCuartelAreaDpto in campos_cursor.fetchall():
        i = i + 1
        print(Gratificacion, TopeImponible, CodHEx1, CodHEx2, CodFamSer, CodFamRep, FactHEx1M, FactHEx2M, TopeCesantia, FormLicenciasR700, CodLabTrato, CiudadEmp, DomicilioEmp, RazonSocialEmp, CodSemCorr, RUTEmp, CodFeriado, CodVacacion, ValorIngresoMinimo, ValorIVA, PrecisionSueldos, AporteIndefinido, AporteFijo, TopeGratificacion, CodColacion, CodMovilizacion, TieneRem700, CodSabado, CodDomingo, OpeContabFin700, CodLicencia, CodCalMermaProd, CtaFacturasPagar, CtaIvaCredito, CtaOtrosImpuestos, CodMoneda, BodegaPpal, CorrConsumos, PorcRetencion, CtaRetencion, CtaHonoraPagar, PathServer, CorrLiqODC, IPServerKupay, NombreMaquina, ConConexionKupay, DiasDeTrabajo, ClaveDesignerKupay, FonoEmp, FaxEmp, GiroEmp, RutRepLegal, PaisEmp, xLista, CorrelativoBaseDatos, Host, ToeMail, EliminaDespliegeValores, EliminaModStockFactura, ODBCDataSource, ODBCName, ODBCPass, ODBCDataSourceF700, ODBCNameF700, ODBCPassF700, PorUnaFin700, DivFin700, UniFin700, IdTesoreriaFin700, IdContabFin700, IdLCoFin700, IdLVeFin700, CtaIESNoRecuperable, CtaIESRecuperable, ProcedimientosGlobal, LeyendaOC, PagoPorHilera, PorcAnticipo, MaximoHE, CodBonoSobreTpo, LeyendaFacturarA, LeyendaAnexa, CodEmp, ImpMembreteDocCpra, MembreteODC, TDFacturaCpra, TDNotaCredito, TDNotaDebito, ValorHEContratista, TDIngreso, TDEgreso, TDTraspaso, TDGuiaCompra, TDAplicacion, CodBonoTratos, PagoTratos, FormAusenciasR700, PathODC, CalculoCargasFicha, ConPreAprobacion, DiasVacaciones, TipoFiniquito, TarjaMesB30, IPServerCampos, NomMaquinaCampos, ClaveDesignerCampos, PathBPA, TopeHExMes, HEx2SinMaximos, FactHEx1D, FactHEx2D, BonoMensual, FactHEx1D2, MesInv, AgnoInv, VersionFIN700, ODBCDsnR700, ODBCNombreR700, ODBCClaveR700, CodEmpR700, TDGuiaDevolucion, CtaFacturasRecibir, TDBoletaHonorario, CtaNotaPorRecibir, CtaResultado, TituloLiquidacion, FactCpraServMaqConDist, TDAjusteInventario, ToleranciaDocum, LabCostoEmpR700, LabFiniquitoR700, FormDiasLiqR700, FormSemCorrR700, TipoProyectoR700, DiasBaseBonoTratos, VersionREM700, GeneraArchivos, RemArchivoPlano, ValidaCuartelAreaDpto)
        sql = "INSERT INTO " + EsquemaBD + ".Parametros (Gratificacion, TopeImponible, CodHEx1, CodHEx2, CodFamSer, " \
                                           "CodFamRep, FactHEx1M, FactHEx2M, TopeCesantia, FormLicenciasR700, CodLabTrato, " \
                                           "CiudadEmp, DomicilioEmp, RazonSocialEmp, CodSemCorr, RUTEmp, CodFeriado, " \
                                           "CodVacacion, ValorIngresoMinimo, ValorIVA, PrecisionSueldos, AporteIndefinido, " \
                                           "AporteFijo, TopeGratificacion, CodColacion, CodMovilizacion, TieneRem700, " \
                                           "CodSabado, CodDomingo, OpeContabFin700, CodLicencia, CodCalMermaProd, " \
                                           "CtaFacturasPagar, CtaIvaCredito, CtaOtrosImpuestos, CodMoneda, BodegaPpal, " \
                                           "CorrConsumos, PorcRetencion, CtaRetencion, CtaHonoraPagar, PathServer, " \
                                           "CorrLiqODC, IPServerKupay, NombreMaquina, ConConexionKupay, DiasDeTrabajo, " \
                                           "ClaveDesignerKupay, FonoEmp, FaxEmp, GiroEmp, RutRepLegal, PaisEmp, xLista, " \
                                           "CorrelativoBaseDatos, Host, ToeMail, EliminaDespliegeValores, " \
                                           "EliminaModStockFactura, ODBCDataSource, ODBCName, ODBCPass,ODBCDataSourceF700," \
                                           "ODBCNameF700, ODBCPassF700, PorUnaFin700, DivFin700, UniFin700, " \
                                           "IdTesoreriaFin700, IdContabFin700, IdLCoFin700, IdLVeFin700, " \
                                           "CtaIESNoRecuperable, CtaIESRecuperable, ProcedimientosGlobal, LeyendaOC, " \
                                           "PagoPorHilera, PorcAnticipo, MaximoHE, CodBonoSobreTpo, LeyendaFacturarA," \
                                           "LeyendaAnexa, CodEmp, ImpMembreteDocCpra, MembreteODC, TDFacturaCpra," \
                                           " TDNotaCredito, TDNotaDebito, ValorHEContratista, TDIngreso, TDEgreso, " \
                                           "TDTraspaso, TDGuiaCompra, TDAplicacion, CodBonoTratos, PagoTratos, " \
                                           "FormAusenciasR700, PathODC, CalculoCargasFicha, ConPreAprobacion, " \
                                           "DiasVacaciones, TipoFiniquito, TarjaMesB30, IPServerCampos, NomMaquinaCampos, " \
                                           "ClaveDesignerCampos, PathBPA, TopeHExMes, HEx2SinMaximos, FactHEx1D, " \
                                           "FactHEx2D, BonoMensual, FactHEx1D2, MesInv, AgnoInv, VersionFIN700, ODBCDsnR700," \
                                           " ODBCNombreR700, ODBCClaveR700, CodEmpR700, TDGuiaDevolucion, " \
                                           "CtaFacturasRecibir, TDBoletaHonorario, CtaNotaPorRecibir, CtaResultado, " \
                                           "TituloLiquidacion, FactCpraServMaqConDist, TDAjusteInventario, ToleranciaDocum," \
                                           " LabCostoEmpR700, LabFiniquitoR700, FormDiasLiqR700, FormSemCorrR700, " \
                                           "TipoProyectoR700, DiasBaseBonoTratos, VersionREM700, GeneraArchivos, " \
                                           "RemArchivoPlano, ValidaCuartelAreaDpto) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s)"
        val = (Gratificacion, TopeImponible, CodHEx1, CodHEx2, CodFamSer, CodFamRep, FactHEx1M, FactHEx2M, TopeCesantia, FormLicenciasR700, CodLabTrato, CiudadEmp, DomicilioEmp, RazonSocialEmp, CodSemCorr, RUTEmp, CodFeriado, CodVacacion, ValorIngresoMinimo, ValorIVA, PrecisionSueldos, AporteIndefinido, AporteFijo, TopeGratificacion, CodColacion, CodMovilizacion, TieneRem700, CodSabado, CodDomingo, OpeContabFin700, CodLicencia, CodCalMermaProd, CtaFacturasPagar, CtaIvaCredito, CtaOtrosImpuestos, CodMoneda, BodegaPpal, CorrConsumos, PorcRetencion, CtaRetencion, CtaHonoraPagar, PathServer, CorrLiqODC, IPServerKupay, NombreMaquina, ConConexionKupay, DiasDeTrabajo, ClaveDesignerKupay, FonoEmp, FaxEmp, GiroEmp, RutRepLegal, PaisEmp, xLista, CorrelativoBaseDatos, Host, ToeMail, EliminaDespliegeValores, EliminaModStockFactura, ODBCDataSource, ODBCName, ODBCPass, ODBCDataSourceF700, ODBCNameF700, ODBCPassF700, PorUnaFin700, DivFin700, UniFin700, IdTesoreriaFin700, IdContabFin700, IdLCoFin700, IdLVeFin700, CtaIESNoRecuperable, CtaIESRecuperable, ProcedimientosGlobal, LeyendaOC, PagoPorHilera, PorcAnticipo, MaximoHE, CodBonoSobreTpo, LeyendaFacturarA, LeyendaAnexa, CodEmp, ImpMembreteDocCpra, MembreteODC, TDFacturaCpra, TDNotaCredito, TDNotaDebito, ValorHEContratista, TDIngreso, TDEgreso, TDTraspaso, TDGuiaCompra, TDAplicacion, CodBonoTratos, PagoTratos, FormAusenciasR700, PathODC, CalculoCargasFicha, ConPreAprobacion, DiasVacaciones, TipoFiniquito, TarjaMesB30, IPServerCampos, NomMaquinaCampos, ClaveDesignerCampos, PathBPA, TopeHExMes, HEx2SinMaximos, FactHEx1D, FactHEx2D, BonoMensual, FactHEx1D2, MesInv, AgnoInv, VersionFIN700, ODBCDsnR700, ODBCNombreR700, ODBCClaveR700, CodEmpR700, TDGuiaDevolucion, CtaFacturasRecibir, TDBoletaHonorario, CtaNotaPorRecibir, CtaResultado, TituloLiquidacion, FactCpraServMaqConDist, TDAjusteInventario, ToleranciaDocum, LabCostoEmpR700, LabFiniquitoR700, FormDiasLiqR700, FormSemCorrR700, TipoProyectoR700, DiasBaseBonoTratos, VersionREM700, GeneraArchivos, RemArchivoPlano, ValidaCuartelAreaDpto)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Parametros: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Parametros', 'Parametros', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla `Periodo`
    i = 0
    campos_cursor.execute('SELECT `NumFolioFC`, `FechaInicio`, `FechaTermino`, `NomPeriodo`, `CodPeriodo`, `DiasPeriodo`, '
                          '`MesPeriodo`, `AgnoPeriodo`, `DiasHabilesPer` FROM Periodo')
    registrosorigen = campos_cursor.rowcount
    print("(81) tabla Periodo")
    print(registrosorigen)
    for NumFolioFC, FechaInicio, FechaTermino, NomPeriodo, CodPeriodo, DiasPeriodo, MesPeriodo, AgnoPeriodo, DiasHabilesPer in campos_cursor.fetchall():
        i = i + 1
        print(NumFolioFC, FechaInicio, FechaTermino, NomPeriodo, CodPeriodo, DiasPeriodo, MesPeriodo, AgnoPeriodo, DiasHabilesPer)
        sql = "INSERT INTO " + EsquemaBD + ".Periodo (NumFolioFC, FechaInicio, FechaTermino, NomPeriodo, CodPeriodo, " \
                                           "DiasPeriodo, MesPeriodo, AgnoPeriodo, DiasHabilesPer) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumFolioFC, FechaInicio, FechaTermino, NomPeriodo, CodPeriodo, DiasPeriodo, MesPeriodo, AgnoPeriodo, DiasHabilesPer)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Periodo: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Periodo', 'Periodo', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()





    # Tabla `PlanCuentas`
    i = 0
    campos_cursor.execute('SELECT `CodCtaCtble`, `TipoCtaCtble`, `NomCtaCtble`, `xAtributos` FROM PlanCuentas')
    registrosorigen = campos_cursor.rowcount
    print("(82) tabla PlanCuentas")
    print(registrosorigen)
    for CodCtaCtble, TipoCtaCtble, NomCtaCtble, xAtributos in campos_cursor.fetchall():
        i = i + 1
        print(CodCtaCtble, TipoCtaCtble, NomCtaCtble, xAtributos)
        sql = "INSERT INTO " + EsquemaBD + ".PlanCuentas (CodCtaCtble, TipoCtaCtble, NomCtaCtble, xAtributos) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (CodCtaCtble, TipoCtaCtble, NomCtaCtble, xAtributos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla PlanCuentas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'PlanCuentas', 'PlanCuentas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla `PptoDetalleCC`
    i = 0
    campos_cursor.execute('SELECT `Agno`, `CodCuartel`, `HasCuartel`, `ValorHa`, `IC_Unico`, `CantidadPpto`, '
                          '`ValorUnitPpto`, `TotalCantidad`, `TotalValor`, `CodigoPpto`, `Mes`, `DescPpto`, '
                          '`RealCantidad`, `RealValor`, `ParaBorrar` FROM PptoDetalleCC')
    registrosorigen = campos_cursor.rowcount
    print("(83) tabla PptoDetalleCC")
    print(registrosorigen)
    for Agno, CodCuartel, HasCuartel, ValorHa, IC_Unico, CantidadPpto, ValorUnitPpto, TotalCantidad, TotalValor, CodigoPpto, Mes, DescPpto, RealCantidad, RealValor, ParaBorrar in campos_cursor.fetchall():
        i = i + 1
        print(Agno, CodCuartel, HasCuartel, ValorHa, IC_Unico, CantidadPpto, ValorUnitPpto, TotalCantidad, TotalValor, CodigoPpto, Mes, DescPpto, RealCantidad, RealValor, ParaBorrar)
        sql = "INSERT INTO " + EsquemaBD + ".PptoDetalleCC (Agno, CodCuartel, HasCuartel, ValorHa, IC_Unico, " \
                                           "CantidadPpto, ValorUnitPpto, TotalCantidad, TotalValor, CodigoPpto, " \
                                           "Mes, DescPpto, RealCantidad, RealValor, ParaBorrar) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Agno, CodCuartel, HasCuartel, ValorHa, IC_Unico, CantidadPpto, ValorUnitPpto, TotalCantidad, TotalValor, CodigoPpto, Mes, DescPpto, RealCantidad, RealValor, ParaBorrar)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla PptoDetalleCC: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'PptoDetalleCC', 'PptoDetalleCC', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla `PptoItems`
    i = 0
    campos_cursor.execute('SELECT `Tipo`, `Estado`, `CodItem`, `DescItem`, `Cantidad`, `ValorUnitario`, `IC_Unico`, '
                          '`CodCultivo` FROM PptoItems')
    registrosorigen = campos_cursor.rowcount
    print("(84) tabla PptoItems")
    print(registrosorigen)
    for Tipo, Estado, CodItem, DescItem, Cantidad, ValorUnitario, IC_Unico, CodCultivo in campos_cursor.fetchall():
        i = i + 1
        print(Tipo, Estado, CodItem, DescItem, Cantidad, ValorUnitario, IC_Unico, CodCultivo)
        sql = "INSERT INTO " + EsquemaBD + ".PptoItems (Tipo, Estado, CodItem, DescItem, Cantidad, ValorUnitario, " \
                                           "IC_Unico, CodCultivo) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Tipo, Estado, CodItem, DescItem, Cantidad, ValorUnitario, IC_Unico, CodCultivo)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla PptoItems: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'PptoItems', 'PptoItems', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    #Tabla  `PptoItemsDetalle`
    i = 0
    campos_cursor.execute('SELECT `IC_Unico`, `Codigo`, `Cantidad`, `ValorUnitario`, `Descripcion` FROM PptoItemsDetalle')
    registrosorigen = campos_cursor.rowcount
    print("(85) tabla PptoItemsDetalle")
    print(registrosorigen)
    for IC_Unico, Codigo, Cantidad, ValorUnitario, Descripcion in campos_cursor.fetchall():
        i = i + 1
        print(IC_Unico, Codigo, Cantidad, ValorUnitario, Descripcion)
        sql = "INSERT INTO " + EsquemaBD + ".PptoItemsDetalle (IC_Unico, Codigo, Cantidad, ValorUnitario, Descripcion) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (IC_Unico, Codigo, Cantidad, ValorUnitario, Descripcion)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla PptoItemsDetalle: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TiposParametros', 'TiposParametros', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `Prioridad`
    i = 0
    campos_cursor.execute('SELECT `CodPrioridad`, `NomPrioridad`, `Dias` FROM Prioridad')
    registrosorigen = campos_cursor.rowcount
    print("(86) tabla Prioridad")
    print(registrosorigen)
    for CodPrioridad, NomPrioridad, Dias in campos_cursor.fetchall():
        i = i + 1
        print(CodPrioridad, NomPrioridad, Dias)
        sql = "INSERT INTO " + EsquemaBD + ".Prioridad (CodPrioridad, NomPrioridad, Dias) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodPrioridad, NomPrioridad, Dias)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Prioridad: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Prioridad', 'Prioridad', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla Produccion
    i = 0
    campos_cursor.execute('SELECT `PrdCod`, `PrdFecha`, `PrdAgno`, `PrdTipo`, `PrdCorrelativo`, `Supervisor`, '
                          '`OperadorGrua`, `TrasladoCosecha`, `Despachadora`, `Anotador`, `NumBins`, '
                          '`Tractorista` FROM Produccion')
    registrosorigen = campos_cursor.rowcount
    print("(87) tabla Produccion")
    print(registrosorigen)
    for PrdCod, PrdFecha, PrdAgno, PrdTipo, PrdCorrelativo, Supervisor, OperadorGrua, TrasladoCosecha, Despachadora, Anotador, NumBins, Tractorista in campos_cursor.fetchall():
        i = i + 1
        print(PrdCod, PrdFecha, PrdAgno, PrdTipo, PrdCorrelativo, Supervisor, OperadorGrua, TrasladoCosecha, Despachadora, Anotador, NumBins, Tractorista)
        sql = "INSERT INTO " + EsquemaBD + ".Produccion (PrdCod, PrdFecha, PrdAgno, PrdTipo, PrdCorrelativo, Supervisor, " \
                                           "OperadorGrua, TrasladoCosecha, Despachadora, Anotador, NumBins, Tractorista) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (PrdCod, PrdFecha, PrdAgno, PrdTipo, PrdCorrelativo, Supervisor, OperadorGrua, TrasladoCosecha, Despachadora, Anotador, NumBins, Tractorista)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Produccion: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Produccion', 'Produccion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla Productos
    i = 0
    campos_cursor.execute('SELECT `CodProd`, `CodFam`, `CodSubFam`, `Unidad`, `Costo`, `NomProd`, `Stock`, `StockMinimo`, '
                          '`Vencimiento`, `AplicaEnf`, `Caracteristicas`, `CorrecMonetaria`, `CodCtaCtble`, `CostoPMP`, '
                          '`ManejaVencimiento`, `ProductoNacional`, `CostoFinal`, `CuentaGastos`, `Tolerancia`, '
                          '`TotalExistenciaVal`, `Carencia`, `Retorno`, `StockCorrec`, `FechaUltCpra`, `TotalExisValo`, '
                          '`TotalBdg`, `StockBdg` FROM Productos')
    registrosorigen = campos_cursor.rowcount
    print("(88) tabla Productos")
    print(registrosorigen)
    for CodProd, CodFam, CodSubFam, Unidad, Costo, NomProd, Stock, StockMinimo, Vencimiento, AplicaEnf, Caracteristicas, CorrecMonetaria, CodCtaCtble, CostoPMP, ManejaVencimiento, ProductoNacional, CostoFinal, CuentaGastos, Tolerancia, TotalExistenciaVal, Carencia, Retorno, StockCorrec, FechaUltCpra, TotalExisValo, TotalBdg, StockBdg in campos_cursor.fetchall():
        i = i + 1
        print(CodProd, CodFam, CodSubFam, Unidad, Costo, NomProd, Stock, StockMinimo, Vencimiento, AplicaEnf, Caracteristicas, CorrecMonetaria, CodCtaCtble, CostoPMP, ManejaVencimiento, ProductoNacional, CostoFinal, CuentaGastos, Tolerancia, TotalExistenciaVal, Carencia, Retorno, StockCorrec, FechaUltCpra, TotalExisValo, TotalBdg, StockBdg)
        sql = "INSERT INTO " + EsquemaBD + ".Productos (CodProd, CodFam, CodSubFam, Unidad, Costo, NomProd, Stock, " \
                                           "StockMinimo, Vencimiento, AplicaEnf, Caracteristicas, CorrecMonetaria, " \
                                           "CodCtaCtble, CostoPMP, ManejaVencimiento, ProductoNacional, CostoFinal, " \
                                           "CuentaGastos, Tolerancia, TotalExistenciaVal, Carencia, Retorno, " \
                                           "StockCorrec, FechaUltCpra, TotalExisValo, TotalBdg, StockBdg) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodProd, CodFam, CodSubFam, Unidad, Costo, NomProd, Stock, StockMinimo, Vencimiento, AplicaEnf, Caracteristicas, CorrecMonetaria, CodCtaCtble, CostoPMP, ManejaVencimiento, ProductoNacional, CostoFinal, CuentaGastos, Tolerancia, TotalExistenciaVal, Carencia, Retorno, StockCorrec, FechaUltCpra, TotalExisValo, TotalBdg, StockBdg)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Productos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Productos', 'Productos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `RangoCargas`
    i = 0
    campos_cursor.execute('SELECT `CodCarga`, `RangoImponible`, `ValorCarga`, `CodRango` FROM RangoCargas')
    registrosorigen = campos_cursor.rowcount
    print("(89) tabla RangoCargas")
    print(registrosorigen)
    for CodCarga, RangoImponible, ValorCarga, CodRango in campos_cursor.fetchall():
        i = i + 1
        print(CodCarga, RangoImponible, ValorCarga, CodRango)
        sql = "INSERT INTO " + EsquemaBD + ".RangoCargas (CodCarga, RangoImponible, ValorCarga, CodRango) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (CodCarga, RangoImponible, ValorCarga, CodRango)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla RangoCargas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'RangoCargas', 'RangoCargas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla `Sector`
    i = 0
    campos_cursor.execute('SELECT `CodSector`, `NomSector` FROM Sector')
    registrosorigen = campos_cursor.rowcount
    print("(90) tabla Sector")
    print(registrosorigen)
    for CodSector, NomSector in campos_cursor.fetchall():
        i = i + 1
        print(CodSector, NomSector)
        sql = "INSERT INTO " + EsquemaBD + ".Sector (CodSector, NomSector) " \
                                           "VALUES (%s, %s)"
        val = (CodSector, NomSector)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Sector: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Sector', 'Sector', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `Servicios`
    i = 0
    campos_cursor.execute('SELECT `CodSer`, `NomSer`, `CodFam`, `CodSubFam`, `Costo`, `Tipo`, `CodCtaCtble` FROM Servicios')
    registrosorigen = campos_cursor.rowcount
    print("(91) tabla Servicios")
    print(registrosorigen)
    for CodSer, NomSer, CodFam, CodSubFam, Costo, Tipo, CodCtaCtble in campos_cursor.fetchall():
        i = i + 1
        print(CodSer, NomSer, CodFam, CodSubFam, Costo, Tipo, CodCtaCtble)
        sql = "INSERT INTO " + EsquemaBD + ".Servicios (CodSer, NomSer, CodFam, CodSubFam, Costo, Tipo, CodCtaCtble) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (CodSer, NomSer, CodFam, CodSubFam, Costo, Tipo, CodCtaCtble)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Servicios: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Servicios', 'Servicios', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    #Tabla `SubFamilias`
    i = 0
    campos_cursor.execute('SELECT `CodSubFam`, `NomSubFam`, `CodFam`, `UnicoSubFam`, `CodCtaCtble` FROM SubFamilias')
    registrosorigen = campos_cursor.rowcount
    print("(92) tabla SubFamilias")
    print(registrosorigen)
    for CodSubFam, NomSubFam, CodFam, UnicoSubFam, CodCtaCtble in campos_cursor.fetchall():
        i = i + 1
        print(CodSubFam, NomSubFam, CodFam, UnicoSubFam, CodCtaCtble)
        sql = "INSERT INTO " + EsquemaBD + ".SubFamilias (CodSubFam, NomSubFam, CodFam, UnicoSubFam, CodCtaCtble) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (CodSubFam, NomSubFam, CodFam, UnicoSubFam, CodCtaCtble)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla SubFamilias: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'SubFamilias', 'SubFamilias', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `TipoCargas`
    i = 0
    campos_cursor.execute('SELECT `CodCarga`, `NomCarga` FROM TipoCargas')
    registrosorigen = campos_cursor.rowcount
    print("(93) tabla TipoCargas")
    print(registrosorigen)
    for CodCarga, NomCarga in campos_cursor.fetchall():
        i = i + 1
        print(CodCarga, NomCarga)
        sql = "INSERT INTO " + EsquemaBD + ".TipoCargas (CodCarga, NomCarga) " \
                                           "VALUES (%s, %s)"
        val = (CodCarga, NomCarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TipoCargas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TipoCargas', 'TipoCargas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `TipoDocumentos`
    i = 0
    campos_cursor.execute('SELECT `TipoDoc`, `NomDoc`, `CuentaDoc`, `IdFin700`, `OpeFin700` FROM TipoDocumentos')
    registrosorigen = campos_cursor.rowcount
    print("(94) tabla TipoDocumentos")
    print(registrosorigen)
    for TipoDoc, NomDoc, CuentaDoc, IdFin700, OpeFin700 in campos_cursor.fetchall():
        i = i + 1
        print(TipoDoc, NomDoc, CuentaDoc, IdFin700, OpeFin700)
        sql = "INSERT INTO " + EsquemaBD + ".TipoDocumentos (TipoDoc, NomDoc, CuentaDoc, IdFin700, OpeFin700) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (TipoDoc, NomDoc, CuentaDoc, IdFin700, OpeFin700)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TipoDocumentos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TipoDocumentos', 'TipoDocumentos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla `TipoPlantas`
    i = 0
    campos_cursor.execute('SELECT `CodTipoPlanta`, `NomTipoPlanta` FROM TipoPlantas')
    registrosorigen = campos_cursor.rowcount
    print("(95) tabla TipoPlantas")
    print(registrosorigen)
    for CodTipoPlanta, NomTipoPlanta in campos_cursor.fetchall():
        i = i + 1
        print(CodTipoPlanta, NomTipoPlanta)
        sql = "INSERT INTO " + EsquemaBD + ".TipoPlantas (CodTipoPlanta, NomTipoPlanta) " \
                                           "VALUES (%s, %s)"
        val = (CodTipoPlanta, NomTipoPlanta)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TipoPlantas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TipoPlantas', 'TipoPlantas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `TramosImpuesto`
    i = 0
    campos_cursor.execute('SELECT `DesdeUTM`, `Factor`, `RebajaUTM`, `UnicoUTM`, `DesdePesos`, `RebajaPesos` FROM TramosImpuesto')
    registrosorigen = campos_cursor.rowcount
    print("(96) tabla TramosImpuesto")
    print(registrosorigen)
    for DesdeUTM, Factor, RebajaUTM, UnicoUTM, DesdePesos, RebajaPesos in campos_cursor.fetchall():
        i = i + 1
        print(DesdeUTM, Factor, RebajaUTM, UnicoUTM, DesdePesos, RebajaPesos)
        sql = "INSERT INTO " + EsquemaBD + ".TramosImpuesto (DesdeUTM, Factor, RebajaUTM, UnicoUTM, DesdePesos," \
                                           " RebajaPesos) VALUES (%s, %s, %s, %s, %s, %s)"
        val = (DesdeUTM, Factor, RebajaUTM, UnicoUTM, DesdePesos, RebajaPesos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla TramosImpuesto: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'TramosImpuesto', 'TramosImpuesto', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()




    # Tabla `ValoresTiposParametros`
    i = 0
    campos_cursor.execute('SELECT CodigoTP, Valor, FechaVigencia FROM ValoresTiposParametros')
    registrosorigen = campos_cursor.rowcount
    print("(97) tabla ValoresTiposParametros")
    print(registrosorigen)
    for CodigoTP, Valor, FechaVigencia in campos_cursor.fetchall():
        i = i + 1
        print(CodigoTP, Valor, FechaVigencia)
        sql = "INSERT INTO " + EsquemaBD + ".ValoresTiposParametros (CodigoTP, Valor, FechaVigencia) " \
                                           "VALUES (%s, %s, %s)"
        val = (CodigoTP, Valor, FechaVigencia)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla ValoresTiposParametros: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ValoresTiposParametros', 'ValoresTiposParametros', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `ValoresUF`
    i = 0
    campos_cursor.execute('SELECT UFDia, UFValor FROM ValoresUF')
    registrosorigen = campos_cursor.rowcount
    print("(98) tabla ValoresUF")
    print(registrosorigen)
    for UFDia, UFValor in campos_cursor.fetchall():
        i = i + 1
        print(UFDia, UFValor)
        sql = "INSERT INTO " + EsquemaBD + ".ValoresUF (UFDia, UFValor) " \
                                           "VALUES (%s, %s)"
        val = (UFDia, UFValor)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla ValoresUF: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ValoresUF', 'ValoresUF', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `ValoresUTM`
    i = 0
    campos_cursor.execute('SELECT MesUTM, AgnoUTM, ValorUTM, UnicoUTM FROM ValoresUTM')
    registrosorigen = campos_cursor.rowcount
    print("(99) tabla ValoresUTM")
    print(registrosorigen)
    for MesUTM, AgnoUTM, ValorUTM, UnicoUTM in campos_cursor.fetchall():
        i = i + 1
        print(MesUTM, AgnoUTM, ValorUTM, UnicoUTM)
        sql = "INSERT INTO " + EsquemaBD + ".ValoresUTM (MesUTM, AgnoUTM, ValorUTM, UnicoUTM) " \
                                           "VALUES (%s, %s, %s, %s)"
        val = (MesUTM, AgnoUTM, ValorUTM, UnicoUTM)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla ValoresUTM: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ValoresUTM', 'ValoresUTM', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla Variedades
    i = 0
    campos_cursor.execute('SELECT CodVariedad, NomVariedad, CodCultivo, TotProd, TotMaqui, TotMMOO, TotCtta, '
                          'TotGastos, TotFactDir, TotCmpte, TotCosto, TotIngre, UtilVarie, KilosCosecha, '
                          'CostoTotalKilos, CostoKilo, IngresoKilos, PromedioKilos, UtilidadKilo, TotMMOOInd, '
                          'Has FROM Variedades')
    registrosorigen = campos_cursor.rowcount
    print("(100) tabla Variedades")
    print(registrosorigen)
    for CodVariedad, NomVariedad, CodCultivo, TotProd, TotMaqui, TotMMOO, TotCtta, TotGastos, TotFactDir, TotCmpte, TotCosto, TotIngre, UtilVarie, KilosCosecha, CostoTotalKilos, CostoKilo, IngresoKilos, PromedioKilos, UtilidadKilo, TotMMOOInd, Has in campos_cursor.fetchall():
        i = i + 1
        if str(CostoKilo) == 'inf':
            CostoKilo = 0
        print(CodVariedad, NomVariedad, CodCultivo, TotProd, TotMaqui, TotMMOO, TotCtta, TotGastos, TotFactDir, TotCmpte, TotCosto, TotIngre, UtilVarie, KilosCosecha, CostoTotalKilos, CostoKilo, IngresoKilos, PromedioKilos, UtilidadKilo, TotMMOOInd, Has)
        sql = "INSERT INTO " + EsquemaBD + ".Variedades (CodVariedad, NomVariedad, CodCultivo, TotProd, TotMaqui, " \
                                           "TotMMOO, TotCtta, TotGastos, TotFactDir, TotCmpte, TotCosto, TotIngre, " \
                                           "UtilVarie, KilosCosecha, CostoTotalKilos, CostoKilo, IngresoKilos, " \
                                           "PromedioKilos, UtilidadKilo, TotMMOOInd, Has) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s)"
        val = (CodVariedad, NomVariedad, CodCultivo, TotProd, TotMaqui, TotMMOO, TotCtta, TotGastos, TotFactDir, TotCmpte, TotCosto, TotIngre, UtilVarie, KilosCosecha, CostoTotalKilos, CostoKilo, IngresoKilos, PromedioKilos, UtilidadKilo, TotMMOOInd, Has)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Variedades: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Variedades', 'Variedades', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla `LaboresMaquina`
    i = 0
    campos_cursor.execute('SELECT CodLabMaq, NomLabMaq, ValorLabMaq FROM LaboresMaquina')
    registrosorigen = campos_cursor.rowcount
    print("(101) tabla LaboresMaquina")
    print(registrosorigen)
    for CodLabMaq, NomLabMaq, ValorLabMaq in campos_cursor.fetchall():
        i = i + 1
        print(CodLabMaq, NomLabMaq, ValorLabMaq)
        sql = "INSERT INTO " + EsquemaBD + ".LaboresMaquina (CodLabMaq, NomLabMaq, ValorLabMaq) VALUES (%s, %s, %s)"
        val = (CodLabMaq, NomLabMaq, ValorLabMaq)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla LaboresMaquina: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'LaboresMaquina', 'LaboresMaquina', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    campos_cursor.close()
    Campos.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas en Campos 6/9")
    exit(1)

if __name__ == "__main__":
    main()