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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallesolicitudalim")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LibroAsistencia")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Liquidacion")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LiquidacionContratista")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LiquidacionODC")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Maquinaria")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Monedas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovCtoFinPersonal")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovHEDia")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovHileraDia")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovInsumos2024")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()


    # Tabla MovCtoFinPersonal
    i = 0
    campos_cursor.execute('SELECT `CodPer`, `FechaInicio`, `FechaTermino`, `UnicoFundo`, `Folio`, `FechaIngreso`, '
                          '`FechaModifica`, `TcCodigo`, `SueldoFijo`, `ValDiaPer`, `PagoTratosImponible`, '
                          '`DiasSemanaPer`, `GrupoC`, `Division`, `CausaLegal_id`, `CausaReal_id` '
                          'FROM MovCtoFinPersonal')
    registrosorigen = campos_cursor.rowcount
    print("(48) tabla MovCtoFinPersonal")
    print(registrosorigen)
    for CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id in campos_cursor.fetchall():
        i = i + 1
        Select = None
        Contrato = None
        Finiquito = None
        print(CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, Select, Contrato, Finiquito, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id)
        sql = "INSERT INTO " + EsquemaBD + ".MovCtoFinPersonal (CodPer, FechaInicio, FechaTermino, " \
                                           "UnicoFundo, Folio, Seleccion, Contrato, Finiquito, FechaIngreso," \
                                           " FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, " \
                                           "DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s)"
        val = (CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, Select, Contrato, Finiquito, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla MovCtoFinPersonal: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'MovCtoFinPersonal', 'MovCtoFinPersonal', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla LibroAsistencia
    i = 0
    campos_cursor.execute('SELECT CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, '
                          'HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, '
                          'FolioLibro, Mes, Agno, Dia FROM LibroAsistencia')
    registrosorigen = campos_cursor.rowcount
    print("(49) tabla LibroAsistencia")
    print(registrosorigen)
    for CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia)
        sql = "INSERT INTO " + EsquemaBD + ".LibroAsistencia (CodPer, Fecha, HoraEntrada, " \
                                           "HoraSalida, CantidadHoras, Jornada, HorasExtra50, " \
                                           "HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, " \
                                           "Mes, Agno, Dia) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s)"
        val = (CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla LibroAsistencia: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'LibroAsistencia', 'LibroAsistencia', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla Liquidacion
    i = 0
    campos_cursor.execute('SELECT Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, '
                          'TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, '
                          'APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, '
                          'TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, '
                          'HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, '
                          'GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, '
                          'CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, '
                          'DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, '
                          'TotSCDiferencial, HoraIngreso, ValorVacaciones FROM Liquidacion')
    registrosorigen = campos_cursor.rowcount
    print("(50) tabla Liquidacion")
    print(registrosorigen)
    for Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, TotSCDiferencial, HoraIngreso, ValorVacaciones in campos_cursor.fetchall():
        i = i + 1
        if str(TotImponible) == 'inf':
            TotImponible = 0
        if str(TotHaberImpo) == 'inf':
            TotHaberImpo = 0
        if str(APago) == 'inf':
            APago = 0
        if str(TotalHaberes) == 'inf':
            TotalHaberes = 0
        if str(TotalTributable) == 'inf':
            TotalTributable = 0
        if str(TotSCDiferencial) == 'inf':
            TotSCDiferencial = 0
        print(Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, TotSCDiferencial, HoraIngreso, ValorVacaciones)
        sql = "INSERT INTO " + EsquemaBD + ".Liquidacion (Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, TotSCDiferencial, HoraIngreso, ValorVacaciones) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, TotSCDiferencial, HoraIngreso, ValorVacaciones)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Liquidacion: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Liquidacion', 'Liquidacion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla LiquidacionContratista
    # i = 0
    campos_cursor.execute('SELECT CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago FROM LiquidacionContratista')
    registrosorigen = campos_cursor.rowcount
    print("(51) tabla LiquidacionContratista")
    print(registrosorigen)
    for CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago in campos_cursor.fetchall():
        i = i + 1
        print(CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago)
        sql = "INSERT INTO " + EsquemaBD + ".LiquidacionContratista (CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla LiquidacionContratista: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'LiquidacionContratista', 'LiquidacionContratista', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla LiquidacionODC
    i = 0
    campos_cursor.execute('SELECT LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, '
                          'TotalCant, TipoLiq  FROM LiquidacionODC')
    registrosorigen = campos_cursor.rowcount
    print("(52) tabla LiquidacionODC")
    print(registrosorigen)
    for LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq in campos_cursor.fetchall():
        i = i + 1
        print(LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq)
        sql = "INSERT INTO " + EsquemaBD + ".LiquidacionODC (LiqODCCorr, Fecha, CodEnc, NumODC, " \
                                           "GlosaLiq, TotalCant, TipoLiq) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla LiquidacionODC: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'LiquidacionODC', 'LiquidacionODC', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla Maquinaria
    i = 0
    campos_cursor.execute('SELECT CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, '
                          'CodCtaCtble, CodCC  FROM Maquinaria')
    registrosorigen = campos_cursor.rowcount
    print("(53) tabla Maquinaria")
    print(registrosorigen)
    for CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC in campos_cursor.fetchall():
        i = i + 1
        print(CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC)
        sql = "INSERT INTO " + EsquemaBD + ".Maquinaria (CodMaq, NomMaq, UnidadMaq, CostoMaq, " \
                                           "HorarioMaq, ConMotor, CodCtaCtble, CodCC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Maquinaria: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Maquinaria', 'Maquinaria', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla Monedas
    i = 0
    campos_cursor.execute('SELECT CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700 FROM Monedas')
    registrosorigen = campos_cursor.rowcount
    print("(54) tabla Monedas")
    print(registrosorigen)
    for CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700 in campos_cursor.fetchall():
        i = i + 1
        print(CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700)
        sql = "INSERT INTO " + EsquemaBD + ".Monedas (CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s)"
        val = (CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Monedas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Monedas', 'Monedas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla  `MovHEDia`
    i = 0
    campos_cursor.execute('SELECT IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST FROM MovHEDia')
    registrosorigen = campos_cursor.rowcount
    print("(55) tabla MovHEDia")
    print(registrosorigen)
    for IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST in campos_cursor.fetchall():
        i = i + 1
        print(IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST)
        sql = "INSERT INTO " + EsquemaBD + ".MovHEDia (IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST) " \
                                           "VALUES (%s, %s, %s, %s, %s) "
        val = (IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla MovHEDia: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'MovHEDia', 'MovHEDia', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla `MovHileraDia`
    i = 0
    campos_cursor.execute('SELECT IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, '
                          'PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia FROM MovHileraDia')
    registrosorigen = campos_cursor.rowcount
    print("(56) tabla MovHileraDia")
    print(registrosorigen)
    for IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia in campos_cursor.fetchall():
        i = i + 1
        print(IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia)
        sql = "INSERT INTO " + EsquemaBD + ".MovHileraDia (IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, " \
                                           "CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, " \
                                           "SaldoTrato, IdMovHileraDia) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla MovHileraDia: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'MovHileraDia', 'MovHileraDia', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla `MovInsumos`
    i = 0
    campos_cursor.execute('SELECT CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, '
                          'CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, '
                          'CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, '
                          'DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC '
                          'FROM MovInsumos where year(Fecha)=2024')
    registrosorigen = campos_cursor.rowcount
    print("(57) tabla MovInsumos")
    print(registrosorigen)
    for CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC in campos_cursor.fetchall():
        i = i + 1
        print(CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC)
        sql = "INSERT INTO " + EsquemaBD + ".MovInsumos (CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, " \
                                           "GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, " \
                                           "BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, " \
                                           "Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, " \
                                           "CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla MovInsumos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'MovInsumos', 'MovInsumos2024', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Cierre de cursores y bases de datos
    campos_cursor.close()
    Campos.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas Campos 5/9")

if __name__ == "__main__":
    main()