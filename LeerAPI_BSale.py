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
MesDeCarga = now.month
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

print("Truncando tablas")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Documento")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Producto")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Cliente")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".tipo_de_pago")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".variante")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".tipo_producto")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".tipo_documento")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".sucursal")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".usuario")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".Pago")
print("Fin truncando tablas")

response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
listaTP = response.json()
i = 0
for item in listaTP["items"]:
    i = i + 1
    attributes = item["attributes"]
    sql = "INSERT INTO " + EsquemaBD + ".tipo_producto (id, name, isEditable, state, " \
                                       "imagestionCategoryId, prestashopCategoryId,  " \
                                       "attributes_href) VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (item["id"], item["name"], f_SQLEsc(item["isEditable"]), f_SQLEsc(item["state"]),
           item["imagestionCategoryId"],
           f_SQLEsc(item["prestashopCategoryId"]), attributes["href"])
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla tipo_producto: ", i)


print("Cargando clientes")
url = "https://api.bsale.cl/v1/clients.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]

localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de API BSALE")
print(localtime)

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/clients.json?limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
    print(url)
    response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
    lista = response.json()
    for item in lista["items"]:
        contact = item["contacts"]
        attributes = item["attributes"]
        addresses = item["addresses"]
        #
        # Ingresando informacion de Clientes
        #
        sql = "SELECT * FROM " + EsquemaBD + ".Cliente where id=" + str(item["id"])
        # print(sql)
        bdg_cursor.execute(sql)
        myresult = bdg_cursor.fetchall()
        if (bdg_cursor.rowcount == 0):
            sql = "INSERT INTO " + EsquemaBD + ".Cliente(id, Code,address,municipality,city,state,firstName," \
                                               "lastName,email,phone,company,note, facebook, twitter,hasCredit, " \
                                               "maxCredit,activity,companyOrPerson,accumulatePoints,points," \
                                               "pointsUpdated, sendDte,isForeigner, prestashopClienId, createdAt, " \
                                               "updatedAt,contacts, attributes,addresses) VALUES " \
                                               "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                               "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (item["id"], item["code"], item["address"], item["municipality"], item["city"],
                   item["state"], item["firstName"], item["lastName"], item["email"], item["phone"],
                   item["company"], item["note"], item["facebook"], item["twitter"], item["hasCredit"],
                   item["maxCredit"], item["activity"], item["companyOrPerson"], item["accumulatePoints"],
                   item["points"], item["pointsUpdated"], item["sendDte"], item["isForeigner"],
                   item["prestashopClienId"], item["createdAt"], item["updatedAt"], contact["href"],
                   attributes["href"], addresses["href"])
            bdg_cursor.execute(sql, val)
            bdg.commit()
    i += limite
print("Fin carga tabla cliente FIN API BSALE 1")

print("Cargando variante")
url = "https://api.bsale.cl/v1/variants.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
listaV = response.json()
totalregistros = listaV["count"]
print('Total de variante :' + str(listaV["count"]))

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/variants.json?limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
    response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
    lista = response.json()

    for item in lista["items"]:
        product = item["product"]
        attribute_values = item["attribute_values"]
        costs = item["costs"]
        sql = "INSERT INTO " + EsquemaBD + ".variante (href,id,description,unlimitedStock," \
                                           "allowNegativeStock,state,barCode,code," \
                                           "imagestionCenterCost,imagestionAccount,imagestionConceptCod," \
                                           "imagestionProyectCod,imagestionCategoryCod,imagestionProductId," \
                                           "serialNumber," \
                                           "prestashopCombinationId,prestashopValueId,product_href,product_id," \
                                           "attribute_values_href,attribute_values_id,costs_href) " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (item["href"], item["id"], f_SQLEsc(item["description"]), f_SQLEsc(item["unlimitedStock"]),
               item["allowNegativeStock"], f_SQLEsc(item["state"]),
               item["barCode"], item["code"], item["imagestionCenterCost"], item["imagestionAccount"],
               item["barCode"], item["imagestionConceptCod"],
               item["imagestionProyectCod"], item["imagestionCategoryCod"],
               item["imagestionProductId"], item["serialNumber"],
               item["prestashopCombinationId"], item["prestashopValueId"],
               product["id"], product["href"], attribute_values["href"], costs["href"])
        bdg_cursor.execute(sql, val)
        bdg.commit()
    i += limite
print("Fin carga tabla pagos")

print("Cargando documentos (Boletas)")
url = "https://api.bsale.cl/v1/documents.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de registros :' + str(totalregistros))

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/documents.json?limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
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
        bdg_cursor.execute(sql, val)
        bdg.commit()
    i += limite
print("Fin carga tabla documento")

print("Cargando tipo de pagos")
url = "https://api.bsale.cl/v1/payment_types.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de tipo de pagos :' + str(lista["count"]))

for item in lista["items"]:
    sql = "INSERT INTO " + EsquemaBD + ".tipo_de_pago(href,id,name,isVirtual,isCheck,maxCheck," \
                                       "isCreditNote,isClientCredit,isCash,isCreditMemo,state," \
                                       "maxClientCuota,ledgerAccount,ledgerCode,isAgreementBank," \
                                       "agreementCode) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (item["href"], item["id"], item["name"], f_SQLEsc(item["isVirtual"]), item["isCheck"],
           f_SQLEsc(item["maxCheck"]), item["isCreditNote"], item["isClientCredit"],
           item["isCash"], item["isCreditMemo"], item["state"], item["maxClientCuota"],
           item["ledgerAccount"], f_SQLEsc(item["ledgerCode"]), item["isAgreementBank"],
           f_SQLEsc(item["agreementCode"]))
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Fin carga tabla tipo_de_pago")




print("Cargando productos")
url = "https://api.bsale.cl/v1/products.json?limit=50&offset=0&access_token=" + ACCESS_TOKEN_BSALE
response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
lista = response.json()
totalregistros = lista["count"]
print('Total de productos :' + str(lista["count"]))

i = 0
while i <= totalregistros:
    url = "https://api.bsale.cl/v1/products.json?limit=50&offset=" + str(i) + "&access_token=" + ACCESS_TOKEN_BSALE
    response = requests.get(url, headers={'access_token': ACCESS_TOKEN_BSALE, 'accept': 'application/xml'})
    lista = response.json()
    for item in lista["items"]:
        product_type = item["product_type"]
        variants = item["variants"]
        product_taxes = item["product_taxes"]
        sql = "INSERT INTO " + EsquemaBD + ".Producto (href,Id,name,description,classification,ledgerAccount," \
                                           "costCenter,allowDecimal,stockControl,printDetailPack,state, " \
                                           "prestashopProductId,presashopAttributeId,product_type_href," \
                                           "product_type_id,variants_href,product_taxes_href) " \
                                           "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (item["href"], item["id"], item["name"], f_SQLEsc(item["description"]), item["classification"],
               item["ledgerAccount"], item["costCenter"], item["allowDecimal"], item["stockControl"],
               item["printDetailPack"], item["state"], item["prestashopProductId"], item["presashopAttributeId"],
               product_type["href"], product_type["id"], variants["href"], product_taxes["href"])
        bdg_cursor.execute(sql, val)
        bdg.commit()
    i += limite
print("Fin carga tabla productos")

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

print("Borrado tabla detalle con información del año 2023")
bdg_cursor.execute(
    "delete " + EsquemaBD + ".detalle from " + EsquemaBD + ".detalle where detalle.document_id in "
                                                           "(SELECT distinct id from " + EsquemaBD + ".documento where year(FROM_UNIXTIME(generationDate)) = "
    + str(AgnoACarga) + " and month(FROM_UNIXTIME(generationDate))=" + str(MesDeCarga) + ")")
print("Fin borrado año 2023 en tabla detalle")

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

# Truncacion de fecha carga
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".FechaCargaInformacion")

# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga API Bsale'
fecha = time.localtime(time.time())
sql = "INSERT INTO " + EsquemaBD + ".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()

bdg.close()
bdg_cursor.close()

# Envio de mail con aviso de termino de ejecución script

email_subject = "Aviso final ejecución script BSALE en DL"
message = EmailMessage()
message['Subject'] = email_subject
message['From'] = sender_email
message['To'] = receiver_email
message.set_content("Aviso termino de ejecución script")
server = smtplib.SMTP(email_smtp, 587) # Set smtp server and port
server.ehlo() # Identify this client to the SMTP server
server.starttls() # Secure the SMTP connection
server.login(sender_email, email_pass) # Login to email account
server.send_message(message) # Send email
server.quit() # Close connection to server