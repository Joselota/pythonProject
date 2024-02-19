import os
from DatosConexion.VG import sender_email, email_pass, receiver_email, email_smtp
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
os.system('python CargarTablas.py')  #
envio_mail("Aviso fin ejecución script CargarTablas en DL")

os.system('python Embalaje.py')  # ok
envio_mail("Aviso fin ejecución script Embalaje en DL")

os.system('python vinosembo.py')  # ok
envio_mail("Aviso fin ejecución script vinosembo en DL")

os.system('python CargarDetallePedido.py')  #
envio_mail("Aviso fin ejecución script CargarDetallePedido en DL")

os.system('python CargaEspecial.py')  #
envio_mail("Aviso fin ejecución script CargaEspecial en DL")

os.system('python TablasGuiaDespacho.py')  #
envio_mail("Aviso fin ejecución script TablasGuiaDespacho en DL")

envio_mail("Fin mail 2")

print("Fin carga desde Kupay parte1")
exit(1)

