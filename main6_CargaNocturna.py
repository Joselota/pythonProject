import os

from ScriptSecundarios.Kupay import Detall_embGD, CargaGuiaGComp, ScriptCargaEMB
from ScriptSecundarios.Kupay import CargaFacturas, aporteFactura
from ScriptSecundarios.Kupay import Produccion, CargaCostosVinos, ScriptVC, barricas, CargarTMovimPedido
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


print("Fin carga desde Kupay main5")

CargaFacturas.main()
print("Fin ejecutando Carga CargaFacturas")
aporteFactura.main()
print("Fin ejecutando Carga aporteFactura")
Produccion.main()
print("Fin ejecutando Carga Produccion")
CargaCostosVinos.main()
print("Fin ejecutando Carga CargaCostosVinos")
ScriptVC.main()
print("Fin ejecutando Carga ScriptVC")
barricas.main()
print("Fin ejecutando Carga barricas")
CargarTMovimPedido.main()
print("Fin ejecutando Carga CargarTMovimPedido")

print("Fin carga desde Kupay main6")

