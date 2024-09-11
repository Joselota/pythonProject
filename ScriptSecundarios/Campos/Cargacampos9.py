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
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".MovDiaPer")
print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
Campos = pyodbc.connect('DSN=CamposV3')
campos_cursor = Campos.cursor()

# Tabla `MovDiaPer`
i = 0
campos_cursor.execute('SELECT Fecha, CodPer, NLinea, TotalDia, TotalDiaCant, Estado, TotDiaSinHE, CodMovDia,'
                      ' DiaSemana, TotalBonos, TotalHE, Folio FROM MovDiaPer')
registrosorigen = campos_cursor.rowcount
print("(104) tabla MovDiaPer")
print(registrosorigen)
for Fecha, CodPer, NLinea, TotalDia, TotalDiaCant, Estado, TotDiaSinHE, CodMovDia, DiaSemana, TotalBonos, TotalHE, Folio in campos_cursor.fetchall():
    i = i + 1
    if str(TotalDia) == 'inf':
        TotalDia = 0
    if str(TotDiaSinHE) == 'inf':
        TotDiaSinHE = 0
    print(Fecha, CodPer, NLinea, TotalDia, TotalDiaCant, Estado, TotDiaSinHE, CodMovDia, DiaSemana, TotalBonos, TotalHE, Folio)
    sql = "INSERT INTO " + EsquemaBD + ".MovDiaPer (Fecha, CodPer, NLinea, TotalDia, TotalDiaCant, Estado, TotDiaSinHE," \
                                       " CodMovDia, DiaSemana, TotalBonos, TotalHE, Folio) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (Fecha, CodPer, NLinea, TotalDia, TotalDiaCant, Estado, TotDiaSinHE, CodMovDia, DiaSemana, TotalBonos, TotalHE, Folio)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla MovDiaPer: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'MovDiaPer', 'MovDiaPer', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()


# Cierre de cursores y bases de datos
campos_cursor.close()
Campos.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")
envio_mail("Fin proceso de Cargar Tablas en Campos 9/9")
exit(1)