#SCRIPT DESIGNADO PARA INSERTAR DATOS DE LA API VIUMANENT EN BASE DE DATOS DEL AÑO 2022
#DATOS INGRESADOS SON:DISTRIBUCION/ARTICULOS - DISTRIBUCION/CLIENTES - RETAIL/ARTICULOS - RETAIL/CLIENTES
import pymysql
import requests
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

def SQLEsc(s):
    if s == None:
        return "NULL"
    else:
        return s

# VariablesGlobals
EsquemaBD = "stagecavas"
Periodo = '23'
fechacarga = datetime.datetime.now()


## Inicializar variables
lista = []

## Base de datos de Gestion (donde se cargarán los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

#Truncacion de la tabla distribucion-articulos año 2023
print("Truncando tabla distribucion_articulos23")
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".distribucion_articulos"+Periodo)
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".distribucion_clientes"+Periodo)
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".retail_articulos"+Periodo)
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".retail_clientes"+Periodo)
print("Fin truncando tablas")

#Obtencion de informacion de la API
url = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/distribucion/articulos/A20DE50F-CB33-46E7-B015-31BA48B680D0/20"+Periodo+"0101/20"+Periodo+"1231" #desde el 01-01-2021 hasta ACTUAL
print(url)
print("1 ****************")
response = requests.get(url)
lista = response.json()
#Informacion que recibe la API
dis_arti22 = lista["DATA"]
#consulta de informacion especifica de la API
#D=Distribucion
#Articulo
print("2 ****************")
for DA2022 in dis_arti22: #ciclo para obtener los objetos
    nomar = DA2022["NombreArticulo"]
    periodoar = DA2022["Periodo"]
    canaldisar = DA2022["CanalDistribucion"]
    totalnetoar = DA2022["TotalNeto"]
    totalcantidadar = DA2022["TotalCantidad"]
    print(nomar,periodoar,canaldisar,totalnetoar,totalcantidadar)
    sql = "INSERT INTO "+EsquemaBD+".distribucion_articulos"+Periodo+" (NombreArticulo, Periodo, CanalDistribucion, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s)"  # Query para instertar en la tabla
    val = (nomar, periodoar, canaldisar, totalnetoar, totalcantidadar)  # valores que reemplazan( los %s)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()



#Obtencion de informacion de la API
urldc = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/distribucion/clientes/A20DE50F-CB33-46E7-B015-31BA48B680D0/20"+Periodo+"0101/20"+Periodo+"1231" #desde el 01-01-2021 hasta ACTUAL
response = requests.get(urldc)
lista = response.json()
#Informacion que recibe la API
dis_cli22 = lista["DATA"]
#consulta de informacion especifica de la API
#D=Distribucion
#Arcticulo
for DC2022 in dis_cli22: #ciclo para obtener los objetos
    canalretail = DC2022["CanalRetail"]
    periodo = DC2022["Periodo"]
    rutcliente = DC2022["RutCliente"]
    nomcliente = DC2022["NombreCliente"]
    nomarticulo = DC2022["NombreArticulo"]
    totalneto = DC2022["TotalNeto"]
    totalcantidad = DC2022["TotalCantidad"]
    sql = "INSERT INTO "+EsquemaBD+".distribucion_clientes"+Periodo+" (CanalRetail, Periodo, RutCliente, NombreCliente, NombreArticulo,TotalNeto,TotalCantidad) VALUES (%s,%s,%s,%s,%s,%s,%s)"  # Query para instertar en la tabla
    val = (canalretail,periodo,rutcliente,nomcliente,nomarticulo,totalneto,totalcantidad)  # valores que reemplazan( los %s)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()
#Obtencion de informacion de la API
urlra = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/retail/articulos/A20DE50F-CB33-46E7-B015-31BA48B680D0/20"+Periodo+"0101/20"+Periodo+"1231" #desde el 01-01-2021 hasta actualidad
response = requests.get(urlra)
lista = response.json()
#Informacion que recibe la API
re_art21 = lista["DATA"]
#consulta de informacion especifica de la API
#D=Distribucion
#Arcticulo
for RA2022 in re_art21: #ciclo para obtener los objetos
    nomarti = RA2022["NombreArticulo"]
    periodo = RA2022["Periodo"]
    canaldistr = RA2022["CanalDistribucion"]
    totalneto = RA2022["TotalNeto"]
    totalcantidad= RA2022["TotalCantidad"]
    sql = "INSERT INTO "+EsquemaBD+".retail_articulos"+Periodo+" (NombreArticulo, Periodo, CanalDistribucion, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s)"  # Query para instertar en la tabla
    val = (nomarti, periodo, canaldistr, totalneto, totalcantidad)  # valores que reemplazan( los %s)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()
#Obtencion de informacion de la API
urlrc = "http://www.wineoff.cl/posws/rest/portalvina/load/@C4v45@/retail/clientes/A20DE50F-CB33-46E7-B015-31BA48B680D0/20"+Periodo+"0101/20"+Periodo+"1231" #desde el 01-01-2021 hasta 31-12-2021
response = requests.get(urlrc)
lista = response.json()
#Informacion que recibe la API
re_cli22 = lista["DATA"]
#consulta de informacion especifica de la API
#D=Distribucion
#Arcticulo
for RC2022 in re_cli22: #ciclo para obtener los objetos
    canalretail = RC2022["CanalRetail"]
    periodo = RC2022["Periodo"]
    rutcliente = RC2022["RutCliente"]
    nomcliente = RC2022["NombreCliente"]
    nomarticulo= RC2022["NombreArticulo"]
    totalneto = RC2022["TotalNeto"]
    totalcantidad = RC2022["TotalCantidad"]
    sql = "INSERT INTO "+EsquemaBD+".retail_clientes"+Periodo+" (Canalretail, Periodo, RutCliente, NombreCliente, NombreArticulo, TotalNeto, TotalCantidad) VALUES (%s,%s,%s,%s,%s,%s,%s)"  # Query para instertar en la tabla
    val = (canalretail, periodo, rutcliente, nomcliente, nomarticulo,totalneto,totalcantidad)  # valores que reemplazan( los %s)
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()


print("FIN DE DATOS")
#Truncacion de fecha carga
print("truncando fecha carga")
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".registro_datos")
print("Fin truncando tablas")

#registro de los datos
Proceso = 'Carga 1'
Descripcion = 'Registro de carga'
fecha = time.localtime(time.time())
sql = "INSERT INTO "+EsquemaBD+".registro_datos (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso,Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()
