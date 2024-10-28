from ScriptSecundarios.Kupay import CargarTablas
from ScriptSecundarios.Kupay import Softland2
from ScriptSecundarios.Bcentral import EstadisticasBCentral
from ScriptSecundarios.Kupay import ProcesosCobranza
from ScriptSecundarios.Kupay import RPA
from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage


print(" Ejecutando carga desde Kupay ")

CargarTablas.main()

Softland2.main()

EstadisticasBCentral.main()

ProcesosCobranza.main()

RPA.main()

exit(1)
