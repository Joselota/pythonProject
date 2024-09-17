import os
from ScriptSecundarios.Kupay import ProcesosCobranza
from ScriptSecundarios.Kupay import CargarTablas
from ScriptSecundarios.Kupay import Softland2
from DatosConexion.VG import sender_email, email_pass, email_smtp
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

print("Inicio ejecutando Carga Info medio día")

Softland2.main()
envio_mail("Fin ejecutando Carga Softland2")
CargarTablas.main()
envio_mail("Fin ejecutando Carga CargarTablas")
ProcesosCobranza.main()
envio_mail("Fin ejecutando Carga ProcesosCobranza")

print("Fin carga Info medio día")
exit(1)



