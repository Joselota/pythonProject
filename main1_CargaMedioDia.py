import os
from ScriptSecundarios.Kupay import ProcesosCobranza
from ScriptSecundarios.Kupay import CargarTablas
from ScriptSecundarios.Kupay import Softland2

print("Inicio ejecutando Carga Info medio día")

Softland2.main()
print("Fin ejecutando Carga Softland2")
CargarTablas.main()
print("Fin ejecutando Carga CargarTablas")
ProcesosCobranza.main()
print("Fin ejecutando Carga ProcesosCobranza")

print("Fin carga Info medio día")
exit(1)



