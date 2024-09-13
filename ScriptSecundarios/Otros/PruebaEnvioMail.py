import os
import smtplib
from email.message import EmailMessage

# Credenciales para envio de correo
sender_email = "finanzas@viumanent.cl"
email_pass = "Lut13115"
receiver_email = "igonzalez@viumanent.cl"
email_smtp = "smtp-mail.outlook.com"

def envio_mail(v_email_subject):
    email_subject = v_email_subject
    message = EmailMessage()
    message['Subject'] = email_subject
    message['From'] = sender_email
    message['To'] = 'igonzalez@viumanent.cl'
    message.set_content("Probando envio de mail con cuenta office365")
    server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
    server.ehlo()  # Identify this client to the SMTP server
    server.starttls()  # Secure the SMTP connection
    server.login(sender_email, email_pass)  # Login to email account
    server.send_message(message)  # Send email
    server.quit()  # Close connection to serve

envio_mail("Next expiration notify")

print("Fin carga desde Kupay parte 2")