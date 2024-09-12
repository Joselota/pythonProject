import os

from ScriptSecundarios.Kupay import Embalaje, GuiaVinos, vinosembo, CargaEspecial, TablasGuiaDespacho
from ScriptSecundarios.Kupay import CargarDetallePedido, Prod_liqu

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


print(" Ejecutando carga desde Kupay ")

Embalaje.main()
GuiaVinos.main()
vinosembo.main()
CargaEspecial.main()
TablasGuiaDespacho.main()
CargarDetallePedido.main()
Prod_liqu.main()

envio_mail("Fin mail 2")
exit(1)
