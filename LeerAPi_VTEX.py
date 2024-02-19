import requests
import pymysql
import time

def SQLEsc(s):
    if s == None:
        return "NULL"
    else:
        return s

## Inicializar variables
lista = []
limite = 50
KEY = "vtexappkey-viumanent-KPFXOF"
TOKEN = "KRFZDIOQWRUDIMRZUUINZELTNZVKWBWCNXISORIEZCEKTSAJIPTDWYRFGLMEMEUUVRPKDZLCLDOXZAAVYMBGISFOGDLCEESUIJOBCQFJEQRSKUFFLRGACNMDMBDANIYH"
per_page = 100
creationDate = "[2022-12-25T21:00:00.000Z TO 2022-12-31T01:59:59.999Z]"

## Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

# VariablesGlobales
IPServidor = "127.0.0.1"
UsuarioBD = "operador"
PasswordBD = "Viu2022"
EsquemaBD = "stagevtex"

## Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

#print("Truncando tablas")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".clientprofiledata")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".client")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".totals")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".items")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".detailsorden")
#bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".marketingData")
#print("Fin truncando tablas")

# url = "https://viumanent.vtexcommercestable.com.br/api/oms/pvt/orders?q=ingrid.gonzalez.solis@gmail.com&orderBy=creationDate,asc"
url = "https://viumanent.vtexcommercestable.com.br/api/oms/pvt/orders?per_page=" + str(per_page) + "&f_creationDate=creationDate:" + str(creationDate) + " "
print(url)
response = requests.get(url,headers={'X-VTEX-API-AppKey': KEY, 'X-VTEX-API-AppToken': TOKEN, 'accept': 'application/json','Content-Type': 'application/json'})

lista = response.json()
paging = lista["paging"]
pages = paging["pages"]
print('Total de registros :' + str(paging["total"]))
print('Total de paginas :' + str(paging["pages"]))
print('Pagina actual :' + str(paging["currentPage"]))

limitepaginas = paging["pages"]
pageactual = 1
i = 1
while i <= limitepaginas:
    print(i)
    url = "https://viumanent.vtexcommercestable.com.br/api/oms/pvt/orders?page=" + str(i) + "&per_page=" + str(
        per_page) + "&f_creationDate=creationDate:" + str(creationDate) + " "
    # url = "https://viumanent.vtexcommercestable.com.br/api/oms/pvt/orders?q=ingrid.gonzalez.solis@gmail.com&orderBy=creationDate,asc"
    lista = requests.get(url,
                         headers={'X-VTEX-API-AppKey': KEY, 'X-VTEX-API-AppToken': TOKEN, 'accept': 'application/json',
                                  'Content-Type': 'application/json'})
    list = lista.json()
    # print(list)
    for item in list["list"]:
        # Consultar detalle de orden
        url = "https://viumanent.vtexcommercestable.com.br/api/oms/pvt/orders/" + str(item["orderId"])
        response = requests.get(url, headers={'X-VTEX-API-AppKey': KEY, 'X-VTEX-API-AppToken': TOKEN,
                                              'accept': 'application/json', 'Content-Type': 'application/json'})
        detalle = response.json()
        print(url)
        sql = "INSERT INTO detailsorden (orderId,sequence,marketplaceOrderId,marketplaceServicesEndpoint,sellerOrderId, origin, affiliateId,salesChannel," \
              "merchantName, status, statusDescription, value,creationDate, lastChange, orderGroup)" \
              " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        val = (SQLEsc(detalle["orderId"]), SQLEsc(detalle["sequence"]), SQLEsc(detalle["marketplaceOrderId"]),
               SQLEsc(detalle["marketplaceServicesEndpoint"]),
               SQLEsc(detalle["sellerOrderId"]), SQLEsc(detalle["origin"]), SQLEsc(detalle["affiliateId"]),
               SQLEsc(detalle["salesChannel"]),
               SQLEsc(detalle["merchantName"]), SQLEsc(detalle["status"]), SQLEsc(detalle["statusDescription"]),
               SQLEsc(detalle["value"]),
               SQLEsc(detalle["creationDate"]), SQLEsc(detalle["lastChange"]), SQLEsc(detalle["orderGroup"]))
        print(sql)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
        print(detalle["creationDate"] + " , " + str(detalle["orderId"]))

        #
        # Ingresando informacion de la compra (TOTALS)
        #
        for total in detalle["totals"]:
            sql = "INSERT INTO totals (id,name,value,idOrden) VALUES (%s,%s,%s,%s)"
            val = (total["id"], total["name"], total["value"], detalle["orderId"])
            # print(val)
            bdg_cursor.execute(sql, val)#
            bdg.commit()

        for items in detalle["items"]:
            sql = "INSERT INTO items (uniqueId,id,productId,ean,lockId,quantity,seller,name,refId,price,listPrice,manualPrice,imageUrl," \
                  "detailUrl,sellerSku,priceValidUntil,commission,tax,preSaleDate,measurementUnit,unitMultiplier," \
                  "sellingPrice,isGift,shippingPrice,rewardValue,freightCommission,taxCode,parentItemIndex,parentAssemblyBinding,callCenterOperator," \
                  "serialNumbers,costPrice,IdOrden) VALUES" \
                  " ( %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                  "%s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (SQLEsc(items["uniqueId"]), items["id"], items["productId"], items["ean"], items["lockId"],
                   items["quantity"],
                   SQLEsc(items["seller"]), items["name"], items["refId"], items["price"], items["listPrice"],
                   SQLEsc(items["manualPrice"]),
                   SQLEsc(items["imageUrl"]), items["detailUrl"], SQLEsc(items["sellerSku"]),
                   SQLEsc(items["priceValidUntil"]), SQLEsc(items["commission"]),
                   SQLEsc(items["tax"]), SQLEsc(items["preSaleDate"]),
                   SQLEsc(items["measurementUnit"]), SQLEsc(items["unitMultiplier"]), SQLEsc(items["sellingPrice"]),
                   SQLEsc(items["isGift"]),
                   SQLEsc(items["shippingPrice"]), SQLEsc(items["rewardValue"]), SQLEsc(items["freightCommission"]),
                   SQLEsc(items["taxCode"]), SQLEsc(items["parentItemIndex"]), SQLEsc(items["parentAssemblyBinding"]),
                   SQLEsc(items["callCenterOperator"]),
                   SQLEsc(items["serialNumbers"]), SQLEsc(items["costPrice"]), SQLEsc(detalle["orderId"]))
            bdg_cursor.execute(sql, val)
            bdg.commit()

        # Ingresando informacion de Clientes
        #
        # print("clientProfileData")
        cliente = detalle["clientProfileData"]
        sql = "SELECT * FROM client where userProfileId='" + SQLEsc(cliente["userProfileId"]) + "'"
        bdg_cursor.execute(sql)
        myresult = bdg_cursor.fetchall()
        # print(sql)

        if (bdg_cursor.rowcount == 0):
            sql = "INSERT INTO client (id,email,firstName,lastName,documentType,document,phone,corporateName," \
                  "tradeName,corporateDocument,stateInscription,corporatePhone,isCorporate,userProfileId,customerClass) VALUES " \
                  " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            val = (cliente["id"], cliente["email"], cliente["firstName"], cliente["lastName"], cliente["documentType"],
                   SQLEsc(cliente["document"]), SQLEsc(cliente["phone"]), SQLEsc(cliente["corporateName"]),
                   SQLEsc(cliente["tradeName"]), SQLEsc(cliente["corporateDocument"]),
                   SQLEsc(cliente["stateInscription"]),
                   SQLEsc(cliente["corporatePhone"]), SQLEsc(cliente["isCorporate"]), SQLEsc(cliente["userProfileId"]),
                   SQLEsc(cliente["customerClass"]))
            bdg_cursor.execute(sql, val)
            bdg.commit()

        sql = "INSERT INTO clientprofiledata (userProfileId,orderId) VALUES (%s,%s)"
        val = (SQLEsc(cliente["userProfileId"]), SQLEsc(detalle["orderId"]))
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # print(detalle["orderId"])
        # clientProfileData
        mktdatadummy = {'utmSource': 'None', 'utmCampaign': 'None', 'coupon': 'None'}
        # print(item)
        if "marketingData" in detalle:
            if detalle["marketingData"] is None:
                marketingData = mktdatadummy
            else:
                marketingData = detalle["marketingData"]
        else:
            marketingData = mktdatadummy
        # print(marketingData)
        sql = "INSERT INTO marketingData (utmSource,utmCampaign, coupon, orderId) VALUES (%s,%s, %s,%s)"
        val = (
            SQLEsc(marketingData["utmSource"]), SQLEsc(marketingData["utmCampaign"]), SQLEsc(marketingData["coupon"]),
            SQLEsc(detalle["orderId"]))
        # print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    i += 1

# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga API VTEX'
fecha = time.localtime(time.time())
sql = "INSERT INTO FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()

bdg.close()
bdg_cursor.close()
