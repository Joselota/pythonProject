import requests
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, ACCESS_TOKEN_BSALE
from Tools.funciones import f_SQLEsc


def json_to_string(json):
    js = str(json)
    ret_str = ""
    for i in js:
        f = ""
        if i == "'":
            if i == "'":
                i = "\""
            f = ""
        ret_str += f + i
    return ret_str


def replace_none_with_empty_str(some_dict):
    return {k: ('' if v is None else v) for k, v in some_dict.items()}


# Inicializar variables locales
lista = []
limite = 50
pricelistsdummy = {'href': 'https://api.bsale.cl/v1/document/0000.json', 'id': '0000','state': '0000', 'coin': '0000', 'details': '0000'}
# VariablesGlobales
EsquemaBD = "stagebsale"

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso 4 API BSALE")
print(localtime)
print("Truncando tablas")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".price_lists")
print("Fin truncando tablas")

print("Cargando tipos de price_lists")
url = "https://api.bsale.cl/v1/price_lists.json?limit=10&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de price_lists :' + str(lista["count"]))

i = 0
for item in lista["items"]:
    i = i + 1
    pricelistsdummy = {'href': 'https://api.bsale.cl/v1/document/0000.json', 'id': '0000','state': '00', 'coin': '00', 'details': '0000'}
    if "item" in item:
        office = json_to_string(item["item"])
    else:
        office = pricelistsdummy

    coin = item["coin"]
    details = item["details"]
# SELECT href, id, name, state, coin, details FROM stagebsale.price_lists;
    sql = "INSERT INTO " + EsquemaBD + ".price_lists (href, id, name, state, coin, details) " \
                                       " VALUES (%s, %s, %s, %s, %s, %s)"

    val = (item["href"], item["id"], item["name"], item["state"], coin["href"], details["href"])
    print(val)
    bdg_cursor.execute(sql, val)
    bdg.commit()


print("Cantidad de registros en la tabla price_lists: ", i)

bdg.close()
bdg_cursor.close()
