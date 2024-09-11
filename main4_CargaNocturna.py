import os
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


os.system('python Controller.py')
envio_mail("Aviso fin ejecución script Controller en DL")
os.system('python Orden.py')
envio_mail("Aviso fin ejecución script Orden en DL")
os.system('python KardexGranel.py')
envio_mail("Aviso fin ejecución script KardexGranel en DL")
os.system('python CargaCostosVinos.py')
envio_mail("Aviso fin ejecución script CargaCostosVinos en DL")
os.system('python ScriptVC.py')
envio_mail("Aviso fin ejecución script ScriptVC en DL")
os.system('python DestinoMezcla.py')
envio_mail("Aviso fin ejecución script DestinoMezcla en DL")
os.system('python CargarTMovimPedido.py')
envio_mail("Aviso fin ejecución script CargarTMovimPedido en DL")

print("Fin carga desde Kupay main 3")
exit(1)
