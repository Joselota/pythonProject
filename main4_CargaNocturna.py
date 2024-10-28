import os

from ScriptSecundarios.Kupay import Controller, Orden, KardexGranel, CargaCostosVinos, ScriptVC
from ScriptSecundarios.Kupay import DestinoMezcla
from ScriptSecundarios.Kupay import CargarTMovimPedido

from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage



print("Inicio ejecutando main4")

Controller.main()

Orden.main()

KardexGranel.main()

CargaCostosVinos.main()

ScriptVC.main()

DestinoMezcla.main()

print("Fin carga desde Kupay main4")
exit(1)
