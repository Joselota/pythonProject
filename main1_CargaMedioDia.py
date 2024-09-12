import os
from ScriptSecundarios.Kupay import ProcesosCobranza
from ScriptSecundarios.Kupay import CargarTablas
from ScriptSecundarios.Kupay import Softland2

print("Inicio ejecutando Carga Info medio día")

CargarTablas.main()
Softland2.main()
ProcesosCobranza.main()

print("Fin carga Info medio día")
exit(1)



