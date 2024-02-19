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

os.system('python CargaGuiaGComp.py')  #
envio_mail("Aviso fin ejecución script CargaGuiaGComp en DL")
os.system('python ScriptCargaEMB.py')  #
envio_mail("Aviso fin ejecución script ScriptCargaEMB en DL")
os.system('python Produccion.py')  #
envio_mail("Aviso fin ejecución script Produccion en DL")
os.system('python CargaFacturas.py')  #
envio_mail("Aviso fin ejecución script CargaFacturas en DL")
os.system('python aporteFactura.py')  #
envio_mail("Aviso fin ejecución script aporteFactura en DL")
print("Fin carga desde Kupay parte 3")
exit(1)
