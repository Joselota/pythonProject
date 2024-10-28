import os

from ScriptSecundarios.Kupay import det_embal, ScriptCargaEMB

from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage


print(" Ejecutando carga desde Kupay ")

det_embal.main()

ScriptCargaEMB.main()

exit(1)
