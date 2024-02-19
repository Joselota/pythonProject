import requests
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, ACCESS_TOKEN_BSALE, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from Tools.funciones import f_SQLEsc

# Inicializar variables
lista = []
limite = 50
AgnoACarga = '2023'
MesDeCarga = '9'

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

# VariablesGlobales
EsquemaBD = "stagebsale"

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

bdg_cursor.execute(
    "delete " + EsquemaBD + ".detalle from " + EsquemaBD + ".detalle where detalle.document_id in "
                                                           "(SELECT distinct id from " + EsquemaBD + ".documento where year(FROM_UNIXTIME(emissionDate)) = "
    + AgnoACarga + " and month(FROM_UNIXTIME(emissionDate))=" + MesDeCarga + ")")
print("Fin borrado año 2023 en tabla detalle")

# Leer tabla de documentos
sql = "SELECT distinct id from " + EsquemaBD + ".documento where year(FROM_UNIXTIME(emissionDate)) = " + \
      AgnoACarga + " and month(FROM_UNIXTIME(emissionDate))=" + MesDeCarga
bdg_cursor.execute(sql)
myresult = bdg_cursor.fetchall()
print('Total de documentos a cargar:', bdg_cursor.rowcount)
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


# Truncacion de fecha carga
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FechaCargaInformacion")

# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga API Bsale'
fecha = time.localtime(time.time())
sql = "INSERT INTO " + EsquemaBD + ".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

bdg.close()
bdg_cursor.close()

# Envio de mail con aviso de termino de ejecución script

email_subject = "Aviso final ejecución script BSale en DL"
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
