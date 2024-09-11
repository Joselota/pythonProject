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


print(" Ejecutando carga desde Kupay ")
os.system('python EstadisticasBCentral.py')
envio_mail("Aviso fin ejecución script EstadisticasBCentral en DL")
os.system('python CargarTablas.py')
envio_mail("Aviso fin ejecución script CargarTablas en DL")
os.system('python Softland2.py')
envio_mail("Aviso fin ejecución script Softland2 en DL")
os.system('python ProcesosCobranza.py')
envio_mail("Aviso fin ejecución script ProcesosCobranza en DL")
os.system('python RPA.py')
envio_mail("Aviso fin ejecución script RPA en DL")
envio_mail("Fin Carga básica nocturna")
exit(1)
