import pyodbc
import pymysql
import time
import requests

def SQLEsc(s):
    if s == None:
        return "NULL"
    else:
        return s

# VariablesGlobales
IPServidor = "127.0.0.1"
UsuarioBD = "operador"
PasswordBD = "Viu2022"
EsquemaBD = "stagecavas"
FechaFinal = "20220624"

## Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".retail")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".distribucion")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".distribucioncliente")
print("Fin del proceso de truncado de tablas en " + EsquemaBD + "")

## Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print ("Fecha y hora de inicio del proceso")
print (localtime)

url = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/Retail/Clientes/A20DE50F-CB33-46E7-B015-31BA48B680D0/20210101/"+str(FechaFinal)
response = requests.get(url, headers={'accept': 'application/xml'})
lista = response.json()
print(url)

for indice in lista["DATA"]:
    sql = "INSERT INTO " + EsquemaBD + ".retail (Canal, Periodo, RutCliente, NombreCliente, NombreArticulo, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s,%s,%s) "
    val = (indice["CanalRetail"], indice["Periodo"], indice["RutCliente"], indice["NombreCliente"], indice["NombreArticulo"], indice["TotalNeto"], indice["TotalCantidad"])
    bdg_cursor.execute(sql, val)
    bdg.commit()

url = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/Distribucion/Articulos/A20DE50F-CB33-46E7-B015-31BA48B680D0/20210101/"+str(FechaFinal)
response = requests.get(url, headers={'accept': 'application/xml'})
lista = response.json()
print(url)

for item in lista["DATA"]:
    sql = "INSERT INTO " + EsquemaBD + ".distribucion (NombreArticulo, Periodo, CanalDistribucion, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s) "
    val = (item["NombreArticulo"], item["Periodo"], item["CanalDistribucion"], item["TotalNeto"], item["TotalCantidad"])
    bdg_cursor.execute(sql, val)
    bdg.commit()

url = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/Distribucion/Clientes/A20DE50F-CB33-46E7-B015-31BA48B680D0/20210101/"+str(FechaFinal)
response = requests.get(url, headers={'accept': 'application/xml'})
lista = response.json()
print(url)

for item in lista["DATA"]:
    sql = "INSERT INTO " + EsquemaBD + ".distribucioncliente (CanalRetail, Periodo, RutCliente, NombreCliente, NombreArticulo, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s,%s,%s) "
    val = (item["CanalRetail"], item["Periodo"], item["RutCliente"], item["NombreCliente"], item["NombreArticulo"], item["TotalNeto"], item["TotalCantidad"])
    bdg_cursor.execute(sql, val)
    bdg.commit()



# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga API Bsale'
fecha = time.localtime(time.time())
sql = "INSERT INTO FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso,Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()