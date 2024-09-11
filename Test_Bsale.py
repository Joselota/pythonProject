import requests
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, ACCESS_TOKEN_BSALE, sender_email, email_pass, \
    receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from Tools.funciones import f_SQLEsc
from datetime import datetime


def json_to_string(json):
    js = str(json)
    ret_str = ""
    for k in js:
        f = ""
        if k == "'":
            if k == "'":
                k = "\""
            f = ""
        ret_str += f + k
    return ret_str


def replace_none_with_empty_str(some_dict):
    return {k: ('' if v is None else v) for k, v in some_dict.items()}


# Inicializar variables locales
now = datetime.now()
AgnoACarga = now.year
MesDeCarga = now.month
# AgnoACarga = '2024'
# MesDeCarga = '06'

print("Periodo de carga : " + str(AgnoACarga) + str(MesDeCarga))

lista = []
limite = 50

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
print("Fecha y hora de inicio del proceso TEST API BSALE")
print(localtime)

print("Truncando tablas")
bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".Documento WHERE year(FROM_UNIXTIME(emissionDate)) = 2024")
print("Fin truncando tablas")


print("Cargando documentos (Boletas)")
url = "https://api.bsale.cl/v1/documents.json?emissiondate>=1704153600&limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de registros :' + str(totalregistros))

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/documents.json?emissiondaterange>=1704067200&limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
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
        document_type = item["document_type"]
        clientedummy = {'href': 'https://api.bsale.cl/v1/clients/0000.json', 'id': '0000'}
        if "client" in item:
            client = item["client"]
        else:
            client = clientedummy
        # client = item["client"]
        office = item["office"]
        user = item["user"]
        coin = item["coin"]
        references = item["references"]
        document_taxes = item["document_taxes"]
        details = item["details"]
        sellers = item["sellers"]
        attributes = item["attributes"]
        rcofDatedummy = "000000000"
        # print(client)
        # print(item["address"])
        if "rcofDate" in item:
            rcofDate = item["rcofDate"]
        else:
            rcofDate = rcofDatedummy
        sql = "INSERT INTO " + EsquemaBD + ".documento (href,id, emissionDate,expirationDate,generationDate," \
                                           "rcofDate,number,serialNumber,totalAmount,netAmount,taxAmount," \
                                           "exemptAmount,notExemptAmount,exportTotalAmount,exportNetAmount," \
                                           "exportTaxAmount,exportExemptAmount," \
                                           "commissionRate,commissionNetAmount,commissionTaxAmount," \
                                           "commissionTotalAmount," \
                                           "percentageTaxWithheld,purchaseTaxAmount,purchaseTotalAmount," \
                                           "address,municipality,city," \
                                           "urlTimbre,urlPublicView,urlPdf,urlPublicViewOriginal," \
                                           "urlPdfOriginal,token,state,urlXml," \
                                           "ted,salesId,informedSii,responseMsgSii,document_type_href," \
                                           "document_type_id,client_href," \
                                           "client_id,office_href,office_id,user_href,user_id,coin_href," \
                                           "coin_id,document_taxes,details," \
                                           "sellers,attributes,references_href) " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                           "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                           "%s,%s,%s,%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (item["href"], item["id"], item["emissionDate"], item["expirationDate"], item["generationDate"], rcofDate,
               item["number"], item["serialNumber"], item["totalAmount"], item["netAmount"], item["taxAmount"],
               item["exemptAmount"], item["notExemptAmount"], item["exportTotalAmount"], item["exportNetAmount"],
               item["exportTaxAmount"], item["exportExemptAmount"], item["commissionRate"], item["commissionNetAmount"],
               item["commissionTaxAmount"], item["commissionTotalAmount"], item["percentageTaxWithheld"],
               item["purchaseTaxAmount"], item["purchaseTotalAmount"], item["address"], item["municipality"],
               item["city"], item["urlTimbre"], item["urlPublicView"], item["urlPdf"], item["urlPublicViewOriginal"],
               item["urlPdfOriginal"], item["token"], item["state"], item["urlXml"], item["ted"], item["salesId"],
               item["informedSii"],
               item["responseMsgSii"], document_type["href"], document_type["id"], client["href"], client["id"],
               office["href"],
               office["id"], user["href"], user["id"], coin["href"], coin["id"], document_taxes["href"],
               details["href"],
               sellers["href"], attributes["href"], references["href"])
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    i += limite
print("Fin carga tabla documento")

bdg.close()
bdg_cursor.close()

# Muestra fecha y hora actual al finalizar el proceso
localtime2 = time.asctime(time.localtime(time.time()))
print("Fecha y hora de finalizacion del proceso")
print(localtime2)

# Envio de mail con aviso de termino de ejecución script

email_subject = "Aviso final ejecución script BSALE en DL"
message = EmailMessage()
message['Subject'] = email_subject
message['From'] = sender_email
message['To'] = receiver_email
message.set_content("Aviso termino de ejecución script")
server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
server.ehlo()  # Identify this client to the SMTP server
server.starttls()  # Secure the SMTP connection
server.login(sender_email, email_pass)  # Login to email account
server.send_message(message)  # Send email
server.quit()  # Close connection to server
