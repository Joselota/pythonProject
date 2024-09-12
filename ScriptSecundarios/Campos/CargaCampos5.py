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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".NominaContratista")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Operacion")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionCtble")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionCtbleTDoc")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionTDoc")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".KardexInsumos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Haberes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesContratista")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesMes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesPer")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HilerasCuartel")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Isapre")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Labores")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LaboresMaquina")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()

    # Tabla NominaContratista
    i = 0
    campos_cursor.execute('SELECT IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta,'
                          ' CodProv, Bloqueado, Campamento, AlimentacionEjecutiva FROM NominaContratista')
    registrosorigen = campos_cursor.rowcount
    print("(58) tabla NominaContratista")
    print(registrosorigen)
    for IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva in campos_cursor.fetchall():
        i = i + 1
        print(IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva)
        sql = "INSERT INTO " + EsquemaBD + ".NominaContratista (IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, " \
                                           "Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, " \
                                           "AlimentacionEjecutiva) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla NominaContratista: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'NominaContratista', 'NominaContratista', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla`Operacion`
    i = 0
    campos_cursor.execute('SELECT CodOperacion, NomOperacion FROM Operacion')
    registrosorigen = campos_cursor.rowcount
    print("(59) tabla Operacion")
    print(registrosorigen)
    for CodOperacion, NomOperacion in campos_cursor.fetchall():
        i = i + 1
        print(CodOperacion, NomOperacion)
        sql = "INSERT INTO " + EsquemaBD + ".Operacion (CodOperacion, NomOperacion) VALUES (%s, %s)"
        val = (CodOperacion, NomOperacion)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Operacion: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Operacion', 'Operacion', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `OperacionCtble`
    i = 0
    campos_cursor.execute('SELECT IdOperCtble, NomOperCtble FROM OperacionCtble')
    registrosorigen = campos_cursor.rowcount
    print("(60) tabla OperacionCtble")
    print(registrosorigen)
    for IdOperCtble, NomOperCtble in campos_cursor.fetchall():
        i = i + 1
        print(IdOperCtble, NomOperCtble)
        sql = "INSERT INTO " + EsquemaBD + ".OperacionCtble (IdOperCtble, NomOperCtble) VALUES (%s, %s)"
        val = (IdOperCtble, NomOperCtble)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla OperacionCtble: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'OperacionCtble', 'OperacionCtble', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `OperacionCtbleTDoc`
    i = 0
    campos_cursor.execute('SELECT TipoDoc, IdOperCtble FROM OperacionCtbleTDoc')
    registrosorigen = campos_cursor.rowcount
    print("(61) tabla OperacionCtbleTDoc")
    print(registrosorigen)
    for TipoDoc, IdOperCtble in campos_cursor.fetchall():
        i = i + 1
        print(TipoDoc, IdOperCtble)
        sql = "INSERT INTO " + EsquemaBD + ".OperacionCtbleTDoc (TipoDoc, IdOperCtble) VALUES (%s, %s)"
        val = (TipoDoc, IdOperCtble)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla OperacionCtbleTDoc: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'OperacionCtbleTDoc', 'OperacionCtbleTDoc', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla `OperacionTDoc`
    i = 0
    campos_cursor.execute('SELECT CodOper, TipoDoc FROM OperacionTDoc')
    registrosorigen = campos_cursor.rowcount
    print("(62) tabla OperacionTDoc")
    print(registrosorigen)
    for CodOper, TipoDoc in campos_cursor.fetchall():
        i = i + 1
        print(CodOper, TipoDoc)
        sql = "INSERT INTO " + EsquemaBD + ".OperacionTDoc (CodOper, TipoDoc) VALUES (%s, %s)"
        val = (CodOper, TipoDoc)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla OperacionTDoc: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'OperacionTDoc', 'OperacionTDoc', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `KardexInsumos`
    i = 0
    campos_cursor.execute('SELECT Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, '
                          'MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, '
                          'PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, '
                          'TipoDocDescripcion, NLineaODC FROM KardexInsumos')
    registrosorigen = campos_cursor.rowcount
    print("(63) tabla KardexInsumos")
    print(registrosorigen)
    for Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC in campos_cursor.fetchall():
        i = i + 1
        if str(ValorPromedio) == 'inf':
            ValorPromedio = 0
        if str(MontoSaldoTemp) == 'inf':
            MontoSaldoTemp = 0
        print(Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC)
        sql = "INSERT INTO " + EsquemaBD + ".KardexInsumos (Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio," \
                                           " Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, " \
                                           "NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, " \
                                           "FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC) " \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s)"
        val = (Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla KardexInsumos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'KardexInsumos', 'KardexInsumos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla Haberes
    i = 0
    campos_cursor.execute('SELECT CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, '
                          'IndemAgnosServ, Formula, FormulaCant, UsaParaGratif FROM Haberes')
    registrosorigen = campos_cursor.rowcount
    print("(64) tabla Haberes")
    print(registrosorigen)
    for CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif in campos_cursor.fetchall():
        i = i + 1
        print(CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif)
        sql = "INSERT INTO " + EsquemaBD + ".Haberes (CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, " \
                                           "IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, " \
                                           "UsaParaGratif) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Haberes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Haberes', 'Haberes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Tabla HaberesContratista
    i = 0
    campos_cursor.execute('SELECT IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, '
                          'NumCtoCtta FROM HaberesContratista')
    registrosorigen = campos_cursor.rowcount
    print("(65) tabla HaberesContratista")
    print(registrosorigen)
    for IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta in campos_cursor.fetchall():
        i = i + 1
        print(IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta)
        sql = "INSERT INTO " + EsquemaBD + ".HaberesContratista (IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla HaberesContratista: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'HaberesContratista', 'HaberesContratista', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    #Tabla HaberesMes
    i = 0
    campos_cursor.execute('SELECT CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, '
                          'Imponible, CodPeriodo, Folio FROM HaberesMes')
    registrosorigen = campos_cursor.rowcount
    print("(66) tabla HaberesMes")
    print(registrosorigen)
    for CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio in campos_cursor.fetchall():
        i = i + 1
        print(CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio)
        sql = "INSERT INTO " + EsquemaBD + ".HaberesMes (CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla HaberesMes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'HaberesMes', 'HaberesMes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla HaberesPer
    i = 0
    campos_cursor.execute('SELECT CodPer, CodHaber, ValorHaber, Imponible FROM HaberesPer')
    registrosorigen = campos_cursor.rowcount
    print("(67) tabla HaberesPer")
    print(registrosorigen)
    for CodPer, CodHaber, ValorHaber, Imponible in campos_cursor.fetchall():
        i = i + 1
        print(CodPer, CodHaber, ValorHaber, Imponible)
        sql = "INSERT INTO " + EsquemaBD + ".HaberesPer (CodPer, CodHaber, ValorHaber, Imponible) VALUES (%s, %s, %s, %s)"
        val = (CodPer, CodHaber, ValorHaber, Imponible)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla HaberesPer: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'HaberesPer', 'HaberesPer', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # Tabla HilerasCuartel
    i = 0
    campos_cursor.execute('SELECT CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, '
                          'Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, '
                          'PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, '
                          'TmpRacimos FROM HilerasCuartel')
    registrosorigen = campos_cursor.rowcount
    print("(68) tabla HilerasCuartel")
    print(registrosorigen)
    for CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos in campos_cursor.fetchall():
        i = i + 1
        print(CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos)
        sql = "INSERT INTO " + EsquemaBD + ".HilerasCuartel (CodCuartel, IDHileraCuartel, PlantasProductivas, " \
                                           "PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, " \
                                           "Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, " \
                                           "RacimosPlanta, TmpPlantas, TmpRacimos) " \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla HilerasCuartel: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'HilerasCuartel', 'HilerasCuartel', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `Isapre`
    i = 0
    campos_cursor.execute('SELECT CodIsa, NomIsa, DctoIsa, CodSuperInt FROM Isapre')
    registrosorigen = campos_cursor.rowcount
    print("(69) tabla Isapre")
    print(registrosorigen)
    for CodIsa, NomIsa, DctoIsa, CodSuperInt in campos_cursor.fetchall():
        i = i + 1
        print(CodIsa, NomIsa, DctoIsa, CodSuperInt)
        sql = "INSERT INTO " + EsquemaBD + ".Isapre (CodIsa, NomIsa, DctoIsa, CodSuperInt) VALUES (%s, %s, %s, %s)"
        val = (CodIsa, NomIsa, DctoIsa, CodSuperInt)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Isapre: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Isapre', 'Isapre', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    #Tabla Labores
    i = 0
    campos_cursor.execute('SELECT CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, '
                          'TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, '
                          'SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto,'
                          'TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, '
                          'FechaTerminoInd, ValorHasInd FROM Labores')
    registrosorigen = campos_cursor.rowcount
    print("(70) tabla Labores")
    print(registrosorigen)
    for CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd in campos_cursor.fetchall():
        i = i + 1
        print(CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd)
        sql = "INSERT INTO " + EsquemaBD + ".Labores (CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, " \
                                           "ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, " \
                                           "Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, " \
                                           "TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, " \
                                           "TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, " \
                                           "FechaTerminoInd, ValorHasInd) " \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla Labores: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'Labores', 'Labores', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Tabla `LaboresMaquina`
    i = 0
    campos_cursor.execute('SELECT CodLabMaq, NomLabMaq, ValorLabMaq FROM LaboresMaquina')
    registrosorigen = campos_cursor.rowcount
    print("(71) tabla LaboresMaquina")
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
    envio_mail("Fin proceso de Cargar Tablas en Campos 4/9")

if __name__ == "__main__":
    main()