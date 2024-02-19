import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

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
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Haberes")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesContratista")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesMes")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HaberesPer")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".HilerasCuartel")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Isapre")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".KardexInsumos")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Labores")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LaboresMaquina")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LibroAsistencia")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Liquidacion")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LiquidacionContratista")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".LiquidacionODC")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Maquinaria")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Monedas")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovCtoFinPersonal")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovHEDia")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovHileraDia")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovInsumos")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".NominaContratista")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Operacion")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionCtble")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionCtbleTDoc")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".OperacionTDoc")

print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
Campos = pyodbc.connect('DSN=CamposV3')
campos_cursor = Campos.cursor()


#SELECT `CodPer`, `FechaInicio`, `FechaTermino`, `UnicoFundo`, `Folio`, `Select`, `Contrato_`, `Finiquito_`, `FechaIngreso`, `FechaModifica`, `TcCodigo`, `SueldoFijo`, `ValDiaPer`, `PagoTratosImponible`, `DiasSemanaPer`, `GrupoC`, `Division`, `CausaLegal_id`, `CausaReal_id` FROM `MovCtoFinPersonal`
i = 0
campos_cursor.execute('SELECT `CodPer`, `FechaInicio`, `FechaTermino`, `UnicoFundo`, `Folio`, `FechaIngreso`, `FechaModifica`, `TcCodigo`, `SueldoFijo`, `ValDiaPer`, `PagoTratosImponible`, `DiasSemanaPer`, `GrupoC`, `Division`, `CausaLegal_id`, `CausaReal_id` FROM MovCtoFinPersonal')
registrosorigen = campos_cursor.rowcount
print("(1) tabla MovCtoFinPersonal")
print(registrosorigen)
for CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id in campos_cursor.fetchall():
    i = i + 1
    Select = None
    Contrato = None
    Finiquito = None
    print(CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, Select, Contrato, Finiquito, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id)
    sql = "INSERT INTO " + EsquemaBD + ".MovCtoFinPersonal (CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, Seleccion, Contrato, Finiquito, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodPer, FechaInicio, FechaTermino, UnicoFundo, Folio, Select, Contrato, Finiquito, FechaIngreso, FechaModifica, TcCodigo, SueldoFijo, ValDiaPer, PagoTratosImponible, DiasSemanaPer, GrupoC, Division, CausaLegal_id, CausaReal_id)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla MovCtoFinPersonal: ", i)

#SELECT `CodPer`, `Fecha`, `HoraEntrada`, `HoraSalida`, `CantidadHoras`, `Jornada`, `HorasExtra50`, `HorasExtras100`, `TotalHE`, `Motivo`, `IDLibro`, `SemanaMes`, `FolioLibro`, `Mes`, `Agno`, `Dia` FROM `LibroAsistencia`
i = 0
campos_cursor.execute('SELECT CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia FROM LibroAsistencia')
registrosorigen = campos_cursor.rowcount
print("(1) tabla LibroAsistencia")
print(registrosorigen)
for CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia in campos_cursor.fetchall():
    i = i + 1
    print(CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia)
    sql = "INSERT INTO " + EsquemaBD + ".LibroAsistencia (CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodPer, Fecha, HoraEntrada, HoraSalida, CantidadHoras, Jornada, HorasExtra50, HorasExtras100, TotalHE, Motivo, IDLibro, SemanaMes, FolioLibro, Mes, Agno, Dia)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla LibroAsistencia: ", i)


#SELECT `Mes`, `CodPer`, `TotalDias`, `TotalSalario`, `DctoAfp`, `DctoIsapre`, `TotImponible`, `TotHaberImpo`, `Anticipo`, `TotalDctos`, `TotalTratos`, `NumCargas`, `APago`, `Agno`, `CodLiq`, `TotHabNoImpo`, `Gratifica`, `SemCorr`, `TotalBonos`, `TotalSemCorr`, `SaldoImpoMin`, `Feriados`, `TotalFeriados`, `TotHorasExtra`, `HorasExtra`, `OtrosDctos`, `TotalCargas`, `Prestamos`, `Calculada`, `TotalHaberes`, `GlosaAPago`, `ValorMinimoDia`, `TotalTributable`, `ImpuestoUnico`, `CesantiaDctoTrab`, `CesantiaApFTrab`, `CesantiaApEmpresa`, `AdicionalIsapre`, `CodPeriodo`, `Folio`, `DctoComisionAfp`, `NLiqPeriodo`, `UnicoFundo`, `FechaIngreso`, `EnviadoRem700`, `TotSCDiferencial`, `HoraIngreso`, `ValorVacaciones` FROM `Liquidacion`
i = 0
campos_cursor.execute('SELECT Mes, CodPer, TotalDias, TotalSalario, DctoAfp, DctoIsapre, TotImponible, TotHaberImpo, Anticipo, TotalDctos, TotalTratos, NumCargas, APago, Agno, CodLiq, TotHabNoImpo, Gratifica, SemCorr, TotalBonos, TotalSemCorr, SaldoImpoMin, Feriados, TotalFeriados, TotHorasExtra, HorasExtra, OtrosDctos, TotalCargas, Prestamos, Calculada, TotalHaberes, GlosaAPago, ValorMinimoDia, TotalTributable, ImpuestoUnico, CesantiaDctoTrab, CesantiaApFTrab, CesantiaApEmpresa, AdicionalIsapre, CodPeriodo, Folio, DctoComisionAfp, NLiqPeriodo, UnicoFundo, FechaIngreso, EnviadoRem700, TotSCDiferencial, HoraIngreso, ValorVacaciones FROM Liquidacion')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Liquidacion")
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

#SELECT `CodContratista`, `FechaInicio`, `Observacion`, `TotalIngresos`, `TotalDescuentos`, `IDLiqContratista`, `FechaTermino`, `Total`, `Fecha`, `CodPeriodo`, `Neto`, `Iva`, `Sub_Total`, `MontoAPago` FROM `LiquidacionContratista`
i = 0
campos_cursor.execute('SELECT CodContratista, FechaInicio, Observacion, TotalIngresos, TotalDescuentos, IDLiqContratista, FechaTermino, Total, Fecha, CodPeriodo, Neto, Iva, Sub_Total, MontoAPago FROM LiquidacionContratista')
registrosorigen = campos_cursor.rowcount
print("(1) tabla LiquidacionContratista")
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

#SELECT `LiqODCCorr`, `Fecha`, `CodEnc`, `NumODC`, `GlosaLiq`, `TotalCant`, `TipoLiq` FROM `LiquidacionODC`
i = 0
campos_cursor.execute('SELECT LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq  FROM LiquidacionODC')
registrosorigen = campos_cursor.rowcount
print("(1) tabla LiquidacionODC")
print(registrosorigen)
for LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq in campos_cursor.fetchall():
    i = i + 1
    print(LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq)
    sql = "INSERT INTO " + EsquemaBD + ".LiquidacionODC (LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (LiqODCCorr, Fecha, CodEnc, NumODC, GlosaLiq, TotalCant, TipoLiq)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla LiquidacionODC: ", i)



#SELECT `CodMaq`, `NomMaq`, `UnidadMaq`, `CostoMaq`, `HorarioMaq`, `ConMotor`, `CodCtaCtble`, `CodCC` FROM `Maquinaria`
i = 0
campos_cursor.execute('SELECT CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC  FROM Maquinaria')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Maquinaria")
print(registrosorigen)
for CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC in campos_cursor.fetchall():
    i = i + 1
    print(CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC)
    sql = "INSERT INTO " + EsquemaBD + ".Maquinaria (CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodMaq, NomMaq, UnidadMaq, CostoMaq, HorarioMaq, ConMotor, CodCtaCtble, CodCC)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla Maquinaria: ", i)


#SELECT `CodMon`, `TasaCbio`, `NombreMon`, `Decimales`, `Simbolo`, `IdFin700` FROM `Monedas`
i = 0
campos_cursor.execute('SELECT CodMon, TasaCbio, NombreMon, Decimales, Simbolo, IdFin700 FROM Monedas')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Monedas")
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


#SELECT `IDLaboresPer`, `TipoHE`, `Cantidad`, `Valor`, `CantidadST` FROM `MovHEDia`
i = 0
campos_cursor.execute('SELECT IDLaboresPer, TipoHE, Cantidad, Valor, CantidadST FROM MovHEDia')
registrosorigen = campos_cursor.rowcount
print("(1) tabla MovHEDia")
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


#SELECT `IDLaboresPer`, `IDHileraCuartel`, `Trato`, `CodCuartel`, `CodTipoPlanta`, `CantRacimo`, `PromRacimo`, `SelectCtta`, `NumCtoCtta`, `SaldoTrato`, `IdMovHileraDia` FROM `MovHileraDia`
i = 0
campos_cursor.execute('SELECT IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia FROM MovHileraDia')
registrosorigen = campos_cursor.rowcount
print("(1) tabla MovHileraDia")
print(registrosorigen)
for IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia in campos_cursor.fetchall():
    i = i + 1
    print(IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia)
    sql = "INSERT INTO " + EsquemaBD + ".MovHileraDia (IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (IDLaboresPer, IDHileraCuartel, Trato, CodCuartel, CodTipoPlanta, CantRacimo, PromRacimo, SelectCtta, NumCtoCtta, SaldoTrato, IdMovHileraDia)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla MovHileraDia: ", i)


#SELECT `CodCom`, `Fecha`, `CodProd`, `Cantidad`, `Sub_Total`, `Costo`, `GlosaKardex`, `CodCuartel`, `CantIngreso`, `CantEgreso`, `Mes`, `NomProd`, `BdgCodigo`, `BdgDestino`, `NumODC`, `ValorMoneda`, `CodCtaCtble`, `CodCC`, `Hora`, `NumExportacion`, `CantidadNC`, `CodFaena`, `IDMovimiento`, `DifCorreccion`, `CodMoneda`, `Compra`, `NumDocAso`, `TipoDocAso`, `IvaRec`, `IvaNoRec`, `NLineaODC` FROM `MovInsumos`
i = 0
campos_cursor.execute('SELECT CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC FROM MovInsumos')
registrosorigen = campos_cursor.rowcount
print("(1) tabla MovInsumos")
print(registrosorigen)
for CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC in campos_cursor.fetchall():
    i = i + 1
    print(CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC)
    sql = "INSERT INTO " + EsquemaBD + ".MovInsumos (CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodCom, Fecha, CodProd, Cantidad, Sub_Total, Costo, GlosaKardex, CodCuartel, CantIngreso, CantEgreso, Mes, NomProd, BdgCodigo, BdgDestino, NumODC, ValorMoneda, CodCtaCtble, CodCC, Hora, NumExportacion, CantidadNC, CodFaena, IDMovimiento, DifCorreccion, CodMoneda, Compra, NumDocAso, TipoDocAso, IvaRec, IvaNoRec, NLineaODC)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla MovInsumos: ", i)

#SELECT `IDNomina`, `NumCtoCtta`, `CodPerCtta`, `NomPerCtta`, `Direccion`, `Ciudad`, `NotaTrabajorCtta`, `CodProv`, `Bloqueado`, `Campamento`, `AlimentacionEjecutiva` FROM `NominaContratista`
i = 0
campos_cursor.execute('SELECT IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva FROM NominaContratista')
registrosorigen = campos_cursor.rowcount
print("(1) tabla NominaContratista")
print(registrosorigen)
for IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva in campos_cursor.fetchall():
    i = i + 1
    print(IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva)
    sql = "INSERT INTO " + EsquemaBD + ".NominaContratista (IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (IDNomina, NumCtoCtta, CodPerCtta, NomPerCtta, Direccion, Ciudad, NotaTrabajorCtta, CodProv, Bloqueado, Campamento, AlimentacionEjecutiva)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla NominaContratista: ", i)


#SELECT `CodOperacion`, `NomOperacion` FROM `Operacion`
i = 0
campos_cursor.execute('SELECT CodOperacion, NomOperacion FROM Operacion')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Operacion")
print(registrosorigen)
for CodOperacion, NomOperacion in campos_cursor.fetchall():
    i = i + 1
    print(CodOperacion, NomOperacion)
    sql = "INSERT INTO " + EsquemaBD + ".Operacion (CodOperacion, NomOperacion) VALUES (%s, %s)"
    val = (CodOperacion, NomOperacion)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla Operacion: ", i)

#SELECT `IdOperCtble`, `NomOperCtble` FROM `OperacionCtble`
i = 0
campos_cursor.execute('SELECT IdOperCtble, NomOperCtble FROM OperacionCtble')
registrosorigen = campos_cursor.rowcount
print("(1) tabla OperacionCtble")
print(registrosorigen)
for IdOperCtble, NomOperCtble in campos_cursor.fetchall():
    i = i + 1
    print(IdOperCtble, NomOperCtble)
    sql = "INSERT INTO " + EsquemaBD + ".OperacionCtble (IdOperCtble, NomOperCtble) VALUES (%s, %s)"
    val = (IdOperCtble, NomOperCtble)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla OperacionCtble: ", i)

#SELECT `TipoDoc`, `IdOperCtble` FROM `OperacionCtbleTDoc`
i = 0
campos_cursor.execute('SELECT TipoDoc, IdOperCtble FROM OperacionCtbleTDoc')
registrosorigen = campos_cursor.rowcount
print("(1) tabla OperacionCtbleTDoc")
print(registrosorigen)
for TipoDoc, IdOperCtble in campos_cursor.fetchall():
    i = i + 1
    print(TipoDoc, IdOperCtble)
    sql = "INSERT INTO " + EsquemaBD + ".OperacionCtbleTDoc (TipoDoc, IdOperCtble) VALUES (%s, %s)"
    val = (TipoDoc, IdOperCtble)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla OperacionCtbleTDoc: ", i)


#SELECT `CodOper`, `TipoDoc` FROM `OperacionTDoc`
i = 0
campos_cursor.execute('SELECT CodOper, TipoDoc FROM OperacionTDoc')
registrosorigen = campos_cursor.rowcount
print("(1) tabla OperacionTDoc")
print(registrosorigen)
for CodOper, TipoDoc in campos_cursor.fetchall():
    i = i + 1
    print(CodOper, TipoDoc)
    sql = "INSERT INTO " + EsquemaBD + ".OperacionTDoc (CodOper, TipoDoc) VALUES (%s, %s)"
    val = (CodOper, TipoDoc)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla OperacionTDoc: ", i)



#SELECT `Fecha`, `NumDoc`, `TipoDoc`, `CodProd`, `BdgCodigo`, `ValorPromedio`, `Ingreso`, `Egreso`, `MontoIngreso`, `MontoEgreso`, `Saldo`, `MontoSaldo`, `Hora`, `NumExportacion`, `NumRegistro`, `PrecioCompra`, `SaldoTemp`, `MontoSaldoTemp`, `FechaIngreso`, `NumInternoDoc`, `TipoDocDescripcion`, `NLineaODC` FROM `KardexInsumos`
i = 0
campos_cursor.execute('SELECT Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC FROM KardexInsumos')
registrosorigen = campos_cursor.rowcount
print("(1) tabla KardexInsumos")
print(registrosorigen)
for Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC in campos_cursor.fetchall():
    i = i + 1
    if str(ValorPromedio) == 'inf':
        ValorPromedio = 0
    if str(MontoSaldoTemp) == 'inf':
        MontoSaldoTemp = 0
    print(Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC)
    sql = "INSERT INTO " + EsquemaBD + ".KardexInsumos (Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC) " \
                                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (Fecha, NumDoc, TipoDoc, CodProd, BdgCodigo, ValorPromedio, Ingreso, Egreso, MontoIngreso, MontoEgreso, Saldo, MontoSaldo, Hora, NumExportacion, NumRegistro, PrecioCompra, SaldoTemp, MontoSaldoTemp, FechaIngreso, NumInternoDoc, TipoDocDescripcion, NLineaODC)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla KardexInsumos: ", i)



#SELECT `CodHaber`, `NomHaber`, `ValorHaber`, `TipoVariable`, `Imponible`, `IndemFerProp`, `Diario`, `IndemAgnosServ`, `Formula`, `FormulaCant`, `UsaParaGratif` FROM `Haberes`
i = 0
campos_cursor.execute('SELECT CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif FROM Haberes')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Haberes")
print(registrosorigen)
for CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif in campos_cursor.fetchall():
    i = i + 1
    print(CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif)
    sql = "INSERT INTO " + EsquemaBD + ".Haberes (CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodHaber, NomHaber, ValorHaber, TipoVariable, Imponible, IndemFerProp, Diario, IndemAgnosServ, Formula, FormulaCant, UsaParaGratif)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla Haberes: ", i)


#SELECT `IDLiqContratista`, `CodFaena`, `CodLabor`, `Unidad`, `Cantidad`, `Valor`, `Total`, `NumCtoCtta` FROM `HaberesContratista`
i = 0
campos_cursor.execute('SELECT IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta FROM HaberesContratista')
registrosorigen = campos_cursor.rowcount
print("(1) tabla HaberesContratista")
print(registrosorigen)
for IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta in campos_cursor.fetchall():
    i = i + 1
    print(IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta)
    sql = "INSERT INTO " + EsquemaBD + ".HaberesContratista (IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    val = (IDLiqContratista, CodFaena, CodLabor, Unidad, Cantidad, Valor, Total, NumCtoCtta)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla HaberesContratista: ", i)


#SELECT `CodMes`, `CodHaber`, `CodPer`, `Cantidad`, `Total`, `MesHM`, `AgnoHM`, `ValorMes`, `Imponible`, `CodPeriodo`, `Folio` FROM `HaberesMes`
i = 0
campos_cursor.execute('SELECT CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio FROM HaberesMes')
registrosorigen = campos_cursor.rowcount
print("(1) tabla HaberesMes")
print(registrosorigen)
for CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio in campos_cursor.fetchall():
    i = i + 1
    print(CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio)
    sql = "INSERT INTO " + EsquemaBD + ".HaberesMes (CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodMes, CodHaber, CodPer, Cantidad, Total, MesHM, AgnoHM, ValorMes, Imponible, CodPeriodo, Folio)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla HaberesMes: ", i)

#SELECT `CodPer`, `CodHaber`, `ValorHaber`, `Imponible` FROM `HaberesPer`
i = 0
campos_cursor.execute('SELECT CodPer, CodHaber, ValorHaber, Imponible FROM HaberesPer')
registrosorigen = campos_cursor.rowcount
print("(1) tabla HaberesPer")
print(registrosorigen)
for CodPer, CodHaber, ValorHaber, Imponible in campos_cursor.fetchall():
    i = i + 1
    print(CodPer, CodHaber, ValorHaber, Imponible)
    sql = "INSERT INTO " + EsquemaBD + ".HaberesPer (CodPer, CodHaber, ValorHaber, Imponible) VALUES (%s, %s, %s, %s)"
    val = (CodPer, CodHaber, ValorHaber, Imponible)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla HaberesPer: ", i)


#SELECT `CodCuartel`, `IDHileraCuartel`, `PlantasProductivas`, `PlantasNoProductivas`, `Cargadores`, `Racimos`, `Temporada`, `CodTipoPlanta`, `Peso`, `PlantasHilera`, `PlantasNoExistentes`, `PlantasUsadas`, `RacimosUsados`, `RacimosPlanta`, `TmpPlantas`, `TmpRacimos` FROM `HilerasCuartel`
i = 0
campos_cursor.execute('SELECT CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos FROM HilerasCuartel')
registrosorigen = campos_cursor.rowcount
print("(1) tabla HilerasCuartel")
print(registrosorigen)
for CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos in campos_cursor.fetchall():
    i = i + 1
    print(CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos)
    sql = "INSERT INTO " + EsquemaBD + ".HilerasCuartel (CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos) " \
                                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodCuartel, IDHileraCuartel, PlantasProductivas, PlantasNoProductivas, Cargadores, Racimos, Temporada, CodTipoPlanta, Peso, PlantasHilera, PlantasNoExistentes, PlantasUsadas, RacimosUsados, RacimosPlanta, TmpPlantas, TmpRacimos)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla HilerasCuartel: ", i)

#SELECT `CodIsa`, `NomIsa`, `DctoIsa`, `CodSuperInt` FROM `Isapre`
i = 0
campos_cursor.execute('SELECT CodIsa, NomIsa, DctoIsa, CodSuperInt FROM Isapre')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Isapre")
print(registrosorigen)
for CodIsa, NomIsa, DctoIsa, CodSuperInt in campos_cursor.fetchall():
    i = i + 1
    print(CodIsa, NomIsa, DctoIsa, CodSuperInt)
    sql = "INSERT INTO " + EsquemaBD + ".Isapre (CodIsa, NomIsa, DctoIsa, CodSuperInt) VALUES (%s, %s, %s, %s)"
    val = (CodIsa, NomIsa, DctoIsa, CodSuperInt)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla Isapre: ", i)

#SELECT `CodLabor`, `NomLabor`, `PrecioLabor`, `TipoLabor`, `TotalLabor`, `ValorHas`, `TotJornadas`, `TotDias`, `FechaInicio`, `FechaTermino`, `ATrato`, `Hileras`, `TotalTrato`, `Racimo`, `CtrlQ`, `SinValorTarja`, `DePacking`, `TotalLaborCtta`, `CodLaborRem700`, `TipoCosto`, `TotalLaborInd`, `TotalLaborCttaInd`, `TotJornadasInd`, `TotDiasInd`, `FechaInicioInd`, `FechaTerminoInd`, `ValorHasInd` FROM `Labores`
i = 0
campos_cursor.execute('SELECT CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd FROM Labores')
registrosorigen = campos_cursor.rowcount
print("(1) tabla Labores")
print(registrosorigen)
for CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd in campos_cursor.fetchall():
    i = i + 1
    print(CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd)
    sql = "INSERT INTO " + EsquemaBD + ".Labores (CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd) " \
                                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodLabor, NomLabor, PrecioLabor, TipoLabor, TotalLabor, ValorHas, TotJornadas, TotDias, FechaInicio, FechaTermino, ATrato, Hileras, TotalTrato, Racimo, CtrlQ, SinValorTarja, DePacking, TotalLaborCtta, CodLaborRem700, TipoCosto, TotalLaborInd, TotalLaborCttaInd, TotJornadasInd, TotDiasInd, FechaInicioInd, FechaTerminoInd, ValorHasInd)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla Labores: ", i)



#SELECT `CodLabMaq`, `NomLabMaq`, `ValorLabMaq` FROM `LaboresMaquina`
i = 0
campos_cursor.execute('SELECT CodLabMaq, NomLabMaq, ValorLabMaq FROM LaboresMaquina')
registrosorigen = campos_cursor.rowcount
print("(1) tabla LaboresMaquina")
print(registrosorigen)
for CodLabMaq, NomLabMaq, ValorLabMaq in campos_cursor.fetchall():
    i = i + 1
    print(CodLabMaq, NomLabMaq, ValorLabMaq)
    sql = "INSERT INTO " + EsquemaBD + ".LaboresMaquina (CodLabMaq, NomLabMaq, ValorLabMaq) VALUES (%s, %s, %s)"
    val = (CodLabMaq, NomLabMaq, ValorLabMaq)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla LaboresMaquina: ", i)


# Cierre de cursores y bases de datos
campos_cursor.close()
Campos.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")
