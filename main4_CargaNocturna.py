import os

from ScriptSecundarios.Kupay import Controller, Orden, KardexGranel, CargaCostosVinos, ScriptVC
from ScriptSecundarios.Kupay import DestinoMezcla
from ScriptSecundarios.Kupay import CargarTMovimPedido

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


print("Inicio ejecutando main4")

Controller.main()
print("Fin ejecutando Carga Controller")

Orden.main()
print("Fin ejecutando Carga Orden")

KardexGranel.main()
print("Fin ejecutando Carga KardexGranel")

CargaCostosVinos.main()
print("Fin ejecutando Carga CargaCostosVinos")

ScriptVC.main()
print("Fin ejecutando Carga ScriptVC")

DestinoMezcla.main()
print("Fin ejecutando Carga DestinoMezcla")

#CargarTMovimPedido.main()
#print("Fin ejecutando Carga CargarTMovimPedido")

print("Fin carga desde Kupay main4")
exit(1)
