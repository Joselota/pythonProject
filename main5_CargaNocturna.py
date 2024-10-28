from ScriptSecundarios.Kupay import Detall_embGD, CargaGuiaGComp
from DatosConexion.VG import sender_email, email_pass, email_smtp
import smtplib
from email.message import EmailMessage



print("Fin carga desde Kupay main5")
Detall_embGD.main()

CargaGuiaGComp.main()

print("Fin carga desde Kupay main5")

exit(1)