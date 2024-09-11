import os

print("Inicio ejecutando Carga Info Cobranza")

os.system('python CargarTablas.py')
os.system('python Softland2.py')
os.system('python ProcesosCobranza.py')

print("Fin carga Info Cobranza")
exit(1)



