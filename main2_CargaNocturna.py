import os

from ScriptSecundarios.Kupay import Embalaje, GuiaVinos, vinosembo, CargaEspecial, TablasGuiaDespacho
from ScriptSecundarios.Kupay import CargarDetallePedido, Prod_liqu

from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage



print(" Ejecutando carga desde Kupay ")

Embalaje.main()

GuiaVinos.main()

vinosembo.main()

CargaEspecial.main()

TablasGuiaDespacho.main()

CargarDetallePedido.main()

Prod_liqu.main()

exit(1)
