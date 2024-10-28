from ScriptSecundarios.Kupay import CargaFacturas, aporteFactura
from ScriptSecundarios.Kupay import Produccion, CargaCostosVinos, ScriptVC, barricas, CargarTMovimPedido
from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage


print("Fin carga desde Kupay main5")

CargaFacturas.main()

aporteFactura.main()

Produccion.main()

CargaCostosVinos.main()

ScriptVC.main()

barricas.main()

CargarTMovimPedido.main()

exit(1)

