import math
import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, email_smtp  # , receiver_email
import smtplib
from email.message import EmailMessage

# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalleODC")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_ordencompra")

# TABLA bdg_detalleodc
i = 0
kupay_cursor.execute('SELECT NumODC, Cantidad, Describe, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, '
                     'Stock, SaldoODC, Unidad, FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, '
                     'ValorImpuesto, TotalImpuesto, NLinea, NLineaSC FROM detalleodc')
registrosorigen = kupay_cursor.rowcount
print("tabla bdg_detalleodc")

for NumODC, Cantidad, Describe_det, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, ValorImpuesto, TotalImpuesto, NLinea, NLineaSC in kupay_cursor.fetchall():
    i = i + 1
    if str(PorcDcto) == '-inf':
        PorcDcto = 0
    sql = "INSERT INTO " + EsquemaBD + ".bdg_detalleodc (NumODC, Cantidad, Describe_det, Valor, Sub_Total, " \
                                       "Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, " \
                                       "FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, " \
                                       "ValorImpuesto, TotalImpuesto, NLinea, NLineaSC)" \
                                       " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s)"
    val = (
    NumODC, Cantidad, Describe_det, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad,
    FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, ValorImpuesto, TotalImpuesto, NLinea, NLineaSC)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla bdg_detalleodc: ", i)

# TABLA ordencompra 38
i = 0
kupay_cursor.execute(
    "SELECT numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, "
    "nomprov, codcc, codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, "
    "estado, fechasolic, prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, "
    "fechacotizacion, porcdcto, codemp, codcondpago, fichaexpo, observasolic,fechaapbsol, fechaestpago, "
    "desde_campos, enviadaf700, origencabid FROM ordencompra")
registrosorigen = kupay_cursor.rowcount
print("(38) tabla bdg_ordencompra")
for numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, nomprov, codcc, \
    codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, estado, fechasolic, \
    prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, fechacotizacion, porcdcto, codemp, \
    codcondpago, fichaexpo, observasolic, fechaapbsol, fechaestpago, desde_campos, enviadaf700, \
    origencabid in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_ordencompra(numodc, fechaodc, fechaentrega, codprov, nota, neto, " \
                                       "iva, total, codemite, lugarentrega, nomprov, codcc, codenc, plazo, " \
                                       "exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, " \
                                       "estado, fechasolic, prioridad, vb2, autoriza2, recibe, nomrecibe, " \
                                       "numsolicitud, cotizacion, fechacotizacion, porcdcto, codemp, codcondpago," \
                                       "fichaexpo, observasolic,fechaapbsol, fechaestpago, desde_campos, " \
                                       "enviadaf700, origencabid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
    val = (numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, nomprov, codcc,
           codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, estado, fechasolic,
           prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, fechacotizacion, porcdcto,
           codemp, codcondpago, fichaexpo, observasolic, fechaapbsol, fechaestpago, desde_campos,
           enviadaf700, origencabid)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla bdg_ordencompra: ", i)

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


