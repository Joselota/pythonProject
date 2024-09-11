from datetime import datetime
import pymysql
import time
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
EsquemaBD = "stagesoftland"

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

localtime = time.asctime(time.localtime(time.time()))
print(localtime)

#print("Ejecutando Procesamiento almacenado que determina Margen Neto (proc_MargenNeto)")
# bdg_cursor.execute("call stagecomercial.proc_MargenNeto;")
bdg_cursor.execute("call stagesoftland.proc_saldo_proveedores;")
#print(Termino de proc_MargenNeto")

localtime = time.asctime(time.localtime(time.time()))
print(localtime)

bdg_cursor.execute(" COMMIT; ")
print("Termino de proceso cobranza")
envio_mail("Fin proceso de cobranza")
exit(1)