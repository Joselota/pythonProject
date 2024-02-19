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
cbook_typedummy = '{"href": "https://api.bsale.cl/v1/book_types/1.json", "id": "1"}'
documentdummy = {'href': 'https://api.bsale.cl/v1/document/0000.json', 'id': '0000'}
documentsdummy = {'href': 'https://api.bsale.cl/v1/documents/0000.json', 'id': '0000'}
# VariablesGlobales
EsquemaBD = "stagebsale"

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso 3 API BSALE")
print(localtime)
print("Truncando tablas")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".tipo_documento")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".sucursal")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".usuario")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Pago")
print("Fin truncando tablas")

print("Cargando tipos de documentos")
url = "https://api.bsale.cl/v1/document_types.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de variante :' + str(lista["count"]))

i = 0
for item in lista["items"]:
    i = i + 1
    clientedummy = {'href': 'https://api.bsale.cl/v1/documents/0.json', 'id': '0000'}
    if "book_type" in item:
        book_type = item["book_type"]
    else:
        book_type = cbook_typedummy

    sql = "INSERT INTO " + EsquemaBD + ".tipo_documento (href, id, name, initialNumber, codeSii, " \
                                       "isElectronicDocument, breakdownTax, use1, isSalesNote, " \
                                       "isExempt, restrictsTax, useClient, messageBodyFormat, " \
                                       "thermalPrinter, state, copyNumber, isCreditNote, continuedHigh, " \
                                       "ledgerAccount, ipadPrint, ipadPrintHigh, restrictClientType, " \
                                       "maxDays, book_type) VALUES (%s, %s, %s, %s, %s, %s, %s," \
                                       "%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (item["href"], item["id"], item["name"], item["initialNumber"], item["codeSii"], item["isElectronicDocument"],
           item["breakdownTax"], item["use"], item["isSalesNote"], item["isExempt"], item["restrictsTax"],
           item["useClient"], "", item["thermalPrinter"],
           item["state"], item["copyNumber"], item["isCreditNote"], item["continuedHigh"],
           item["ledgerAccount"], item["ipadPrint"], item["ipadPrintHigh"], item["restrictClientType"],
           item["maxDays"], json_to_string(book_type))
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla tipo_documento: ", i)

print("Cargando sucursales")
url = "https://api.bsale.cl/v1/offices.json?limit=10&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de sucursales :' + str(lista["count"]))

i = 0
for item in lista["items"]:
    i = i + 1
    clientedummy = {'href': 'https://api.bsale.cl/v1/clients/0000.json', 'id': '0000'}
    if "book_type" in item:
        book_type = item["book_type"]
    else:
        book_type = cbook_typedummy

    sql = "INSERT INTO " + EsquemaBD + ".sucursal (href, id, name, description, address, latitude, " \
                                       "longitude, isVirtual, country, municipality, city, zipCode, " \
                                       "email, costCenter, state, imagestionCellarId, defaultPriceList) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s," \
                                       "%s, %s, %s, %s, %s, %s, %s,%s, %s, %s)"
    val = (item["href"], item["id"], item["name"], item["description"], item["address"], item["latitude"],
           item["longitude"], item["isVirtual"], item["country"], item["municipality"], item["city"],
           item["zipCode"], item["email"],
           item["costCenter"], item["state"], item["imagestionCellarId"], item["defaultPriceList"])
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla sucursales: ", i)

print("Cargando usuarios")
url = "https://api.bsale.cl/v1/users.json?access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de usuario :' + str(lista["count"]))

i = 0
for item in lista["items"]:
    i = i + 1
    officedummy = {'href': 'https://api.bsale.cl/v1/offices/0.json', 'id': '0000'}
    if "office" in item:
        office = json_to_string(item["office"])
    else:
        office = officedummy

    sql = "INSERT INTO " + EsquemaBD + ".usuario (href, id, firstName, lastName, email, state, office) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (item["href"], item["id"], item["firstName"], item["lastName"], item["email"], item["state"],
           office)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla usuario: ", i)

print("Cargando pagos")
url = "https://api.bsale.cl/v1/payments.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de pagos :' + str(lista["count"]))

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/payments.json?limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
    count_error = 1
    while count_error < 5:
        try:
            response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
            lista = response.json()
            break
        except:
            print("ERROR RESPONSE:", str(i), " ERROR==>", count_error, "==> \n", response.content)
            time.sleep(10)
            count_error += 1
            continue

    if count_error == 5:
        print("ERROR RESPONSE:", str(i), "Se continua siguiente registro")
        continue

    for item in lista["items"]:
        payment_type = item["payment_type"]
        office = item["office"]
        user = item["user"]
        if "document" in item:
            document = item["document"]
        else:
            document = documentdummy
        if "documents" in item:
            documents = json_to_string((item["documents"]))[1:len(json_to_string(item["documents"])) - 1]
        else:
            documents = ""

        sql = "INSERT INTO " + EsquemaBD + ".Pago (href,id,recordDate,amount,operationNumber,accountingDate," \
                                           "checkDate,checkNumber,checkAmount,checkTaken,isCreditPayment," \
                                           "createdAt,state,payment_type_href,payment_type_id,office_href," \
                                           "office_id,user_href,user_id,document_href,document_id,documents) " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (item["href"], item["id"], item["recordDate"], f_SQLEsc(item["amount"]), item["operationNumber"],
               item["accountingDate"], item["checkDate"], item["checkNumber"], item["checkAmount"], item["checkTaken"],
               item["isCreditPayment"], item["createdAt"], item["state"],
               payment_type["href"], payment_type["id"], office["href"], office["id"], user["href"],
               user["id"], document["href"], document["id"], documents)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    i += limite
print("Fin carga tabla pagos")

bdg.close()
bdg_cursor.close()
