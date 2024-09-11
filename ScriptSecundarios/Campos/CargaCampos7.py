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
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".detallefactcompracuarteles")
print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
Campos = pyodbc.connect('DSN=CamposV3')
campos_cursor = Campos.cursor()

#SELECT IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas FROM detallefactcompracuarteles;
# TABLA cultivos
i = 0
campos_cursor.execute('SELECT IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas FROM detallefactcompracuarteles')
registrosorigen = campos_cursor.rowcount
print("(102) tabla detallefactcompracuarteles")
print(registrosorigen)
for IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas in campos_cursor.fetchall():
    i = i + 1
    print(IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas)
    sql = "INSERT INTO " + EsquemaBD + ".detallefactcompracuarteles(IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas) " \
                                        "VALUES (%s, %s, %s, %s, %s)"
    val = (IDDetFactCpra, CodCuartel, Costo, Fecha, Hectareas)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla detallefactcompracuarteles: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'detallefactcompracuarteles', 'detallefactcompracuarteles', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()



# Cierre de cursores y bases de datos
campos_cursor.close()
Campos.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")
envio_mail("Fin proceso de Cargar Tablas en Campos 7/9")
exit(1)