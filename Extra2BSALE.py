import requests
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, ACCESS_TOKEN_BSALE, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from Tools.funciones import f_SQLEsc
from datetime import datetime


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
now = datetime.now()
AgnoACarga = now.year
MesDeCarga = 1
print("Periodo de carga : "+str(AgnoACarga) + str(MesDeCarga))

lista = []
limite = 50
#AgnoACarga = '2023'
#MesDeCarga = '12'
url = "https://api.bsale.cl/v1/product_types.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
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
print("Fecha y hora de inicio del proceso 1 API BSALE")
print(localtime)



print("Borrado tabla detalle con información del año 2023")
bdg_cursor.execute(
    "delete " + EsquemaBD + ".detalle from " + EsquemaBD + ".detalle where detalle.document_id in "
                                                           "(SELECT distinct id from " + EsquemaBD + ".documento where year(FROM_UNIXTIME(generationDate)) = "
    + str(AgnoACarga) + " and month(FROM_UNIXTIME(generationDate))=" + str(MesDeCarga) + ")")
print("Fin borrado año 2024 en tabla detalle")

# Leer tabla de documentos
sql = "SELECT distinct id from " + EsquemaBD + ".documento where year(FROM_UNIXTIME(generationDate)) = " + \
      str(AgnoACarga) + " and month(FROM_UNIXTIME(generationDate))=" + str(MesDeCarga)
bdg_cursor.execute(sql)
myresult = bdg_cursor.fetchall()
print('Total de documentos a cargar su detalle:', bdg_cursor.rowcount)
i = bdg_cursor.rowcount + 1
for row in myresult:
    url = "https://api.bsale.cl/v1/documents/" + str(row[0]) + \
          "/details.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
    # print("URL==>", url)
    count_error = 1
    i = i - 1
    while count_error < 5:
        try:
            response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
            lista = response.json()
            break
        except:
            print("ERROR RESPONSE:", row[0], " ERROR==>", count_error, "==> \n", response.content)
            time.sleep(10)
            count_error += 1
            continue

    if count_error == 5:
        print("ERROR RESPONSE:", row[0], "Se continua siguiente registro")
        continue

    for item in lista["items"]:
        print(item)
        variant = item["variant"]
        sql = "INSERT INTO " + EsquemaBD + ".detalle (document_id, href,Id,lineNumber,quantity," \
                                           "netUnitValue,totalUnitValue,netAmount,taxAmount,totalAmount,netDiscount," \
                                           "totalDiscount,variant_id,note,relatedDetailId) " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        val = (str(row[0]), item["href"], item["id"], f_SQLEsc(item["lineNumber"]), f_SQLEsc(item["quantity"]),
               item["netUnitValue"], item["totalUnitValue"], item["netAmount"],
               item["taxAmount"], f_SQLEsc(item["totalAmount"]), f_SQLEsc(item["netDiscount"]),
               f_SQLEsc(item["totalDiscount"]),
               variant["id"], f_SQLEsc(item["note"]), f_SQLEsc(item["relatedDetailId"]))
        # print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Boleta cargada " + str(i))

# Muestra fecha y hora actual al finalizar el proceso
localtime2 = time.asctime(time.localtime(time.time()))
print("Fecha y hora de finalizacion del proceso")
print(localtime2)
