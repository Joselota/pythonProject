import os
from ScriptSecundarios.Kupay import ProcesosCobranza
from ScriptSecundarios.Kupay import CargarTablas
from ScriptSecundarios.Kupay import Softland2
from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage


print("Inicio ejecutando Carga Info medio día")

Softland2.main()

CargarTablas.main()

ProcesosCobranza.main()


print("Fin carga Info medio día")
exit(1)



