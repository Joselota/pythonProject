import requests
import pandas as pd
import pymysql
import time
import json
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, PASSWORD, API_KEY, SHARED_SECRET, merchant, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from Tools.funciones import SQLEsc

def envio_mail(v_email_subject):
    email_subject = v_email_subject
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
    server.quit()  # Close connection to serve

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

# VariablesGlobales
EsquemaBD = "stageshopify"
limit = "50"

## Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

## Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso 1 API SHOPIFY")
print(localtime)

bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".orders")
print("Fin truncando tablas")
url_next = "1"
i = 0
codedummy = '{"code": "", "amount": "0", "percentage": "0"}'
taxlinesdummy = '{"price": "", "rate": "", "title": "", "price_set": {"shop_money": {"amount": "", "currency_code": ""}, "presentment_money": {"amount": "", "currency_code": ""}}, "channel_liable": "False"}'
while url_next != "":
    print(limit)
    if url_next == "1":
        url_ordenes = "https://" + API_KEY + ":" + PASSWORD + "@viu-manent.myshopify.com/admin/api/2023-01/orders.json?status=any&limit=" + limit
    else:
        url_ordenes = url_next
    # print(url_ordenes)
    print("i = " + str(i))
    response = requests.get(url_ordenes, params={'client_id': API_KEY, 'scope': 'read_all_orders'},
                            headers={'Content-Type': 'application/json'})
    ordenes = response.json()
    encabezado = response.headers

    if "Link" in encabezado:
        if "next" in encabezado["Link"]:
            if encabezado["Link"].find('previous') == -1:
                next = response.headers['Link'][20:218]
            else:
                next = response.headers['Link'][257:455]
            url_next = "https://" + API_KEY + ":" + PASSWORD + "@viu-manent." + next
        else:
            url_next = ""
    else:
        url_next = ""
    df_ordenes = pd.DataFrame(ordenes['orders'])
    df_ordenes = df_ordenes.reset_index()

    for index, row in df_ordenes.iterrows():
        if len(row["discount_codes"]) == 0:
            discount_codes = codedummy
        else:
            discount_codes = json_to_string((row["discount_codes"]))[1:len(json_to_string(row["discount_codes"])) - 1]
        for clave in row["client_details"]:
            if row["client_details"][clave] is None:
                row["client_details"][clave] = "None"
        client_details = json_to_string(row["client_details"])

        if row["buyer_accepts_marketing"] is None:
            row["buyer_accepts_marketing"] = "True"
        if row["cancel_reason"] is None:
            row["cancel_reason"] = "None"
        if row["cancelled_at"] is None:
            row["cancelled_at"] = "None"
        if row["cart_token"] is None:
            row["cart_token"] = "None"
        current_total_discounts_set = json_to_string(row["current_total_discounts_set"])
        current_subtotal_price_set = json_to_string(row["current_subtotal_price_set"])
        current_total_price_set = json_to_string(row["current_total_price_set"])
        current_total_tax_set = json_to_string(row["current_total_tax_set"])
        landing_site = json_to_string(row["landing_site"])
        note_attributes = json_to_string(row["note_attributes"])
        payment_gateway_names = json_to_string(row["payment_gateway_names"])
        subtotal_price_set = json_to_string(row["subtotal_price_set"])

        if len(row["tax_lines"]) == 0:
            tax_lines = taxlinesdummy
        else:
            tax_lines = json_to_string((row["tax_lines"]))[1:len(json_to_string(row["tax_lines"])) - 1]
        total_discounts_set = '"' + str(row["total_discounts_set"]) + '"'
        total_line_items_price_set = json_to_string(row["total_line_items_price_set"])
        total_price_set = json_to_string(row["total_price_set"])
        total_shipping_price_set = json_to_string(row["total_shipping_price_set"])
        total_tax_set = json_to_string(row["total_tax_set"])

        for clave in row["billing_address"]:

            if row["billing_address"][clave] is None:
                row["billing_address"][clave] = "None"
            if row["billing_address"][clave] == "O'Higgins":
                row["billing_address"][clave] = "OHiggins"
            if row["billing_address"][clave] == "O higgins":
                row["billing_address"][clave] = "OHiggins"
            if row["billing_address"][clave] == 'Local 26 "San Agustín"':
                row["billing_address"][clave] = 'Local 26 San Agustín'
            if row["billing_address"][clave] == 'Avenida Libertador Bernardo O"higgins 0484':
                row["billing_address"][clave] = 'Avenida Libertador Bernardo O higgins 0484'
            row["billing_address"][clave] = str(row["billing_address"][clave]).replace('"', ' ')
            row["billing_address"][clave] = str(row["billing_address"][clave]).replace("'", '')
            if row["billing_address"][clave] == '\u202a+56\xa09\xa03370\xa05524\u202c':
                row["billing_address"][clave] = " "
            if row["billing_address"][clave] == '9\xa09089\xa03563':
                row["billing_address"][clave] = " "
            # print(row["billing_address"][clave])
        print(row["billing_address"])
        billing_address = json_to_string(row["billing_address"])
        print(billing_address)

        customer = row["customer"]["id"]
        discount_applications = json_to_string(row["discount_applications"])
        fulfillments = json_to_string(row["fulfillments"])
        line_items = json_to_string(row["line_items"])
        payment_terms = json_to_string(row["payment_terms"])
        for index, x in enumerate(row["refunds"]):
            x = replace_none_with_empty_str(x)
            row["refunds"][index] = x;
        if len(row["refunds"]) == 0:
            refunds = codedummy
        else:
            refunds = json_to_string(row["refunds"])

        for clave in row["shipping_address"]:
            if row["shipping_address"][clave] == '9\xa09089\xa03563':
                row["shipping_address"][clave] = " "
            if row["shipping_address"][clave] == '\u202a+56\xa09\xa03370\xa05524\u202c':
                row["shipping_address"][clave] = " "
            if row["shipping_address"][clave] is None:
                row["shipping_address"][clave] = "None"
            if row["shipping_address"][clave] == "O'Higgins":
                row["shipping_address"][clave] = "OHiggins"
            if row["shipping_address"][clave] == "O Higgins":
                row["shipping_address"][clave] = "OHiggins"
            if row["shipping_address"][clave] == 'O"Higgins':
                row["shipping_address"][clave] = "OHiggins"
            if row["shipping_address"][clave] == 'O"higgins':
                row["shipping_address"][clave] = "O Higgins"
            if row["shipping_address"][clave] == "O'higgins":
                row["shipping_address"][clave] = "OHiggins"
            if str(row["shipping_address"][clave]).find("'") > 0:
                # print(str(row["shipping_address"][clave]))
                row["shipping_address"][clave] = str(row["shipping_address"][clave]).replace("'", " ")
            if str(row["shipping_address"][clave]).find('"') > 0:
                # print("Por acá 1")
                # print(str(row["shipping_address"][clave]))
                # print("Por acá 2")
                row["shipping_address"][clave] = str(row["shipping_address"][clave]).replace('"', '')
                row["shipping_address"][clave] = str(row["shipping_address"][clave]).replace('í', 'i')
                # print(str(row["shipping_address"][clave]))
        shipping_address = json_to_string(row["shipping_address"])
        # print("shipping_address = " + shipping_address)

        for index, x in enumerate(row["shipping_lines"]):
            x = replace_none_with_empty_str(x)
            row["shipping_lines"][index] = x;
        if len(row["shipping_lines"]) == 0:
            shipping_lines = codedummy
        else:
            shipping_lines = json_to_string((row["shipping_lines"]))[1:len(json_to_string(row["shipping_lines"])) - 1]

        val = (row["id"], row["admin_graphql_api_id"], row["app_id"], row["browser_ip"], row["buyer_accepts_marketing"],
        row["cancel_reason"], row["cancelled_at"], row["cart_token"], row["checkout_id"], row["checkout_token"],
        client_details, row["currency"], row["closed_at"], row["confirmed"], SQLEsc(row["contact_email"]),
        row["created_at"], row["current_subtotal_price"], current_subtotal_price_set,
        row["current_total_discounts"], current_total_discounts_set, row["current_total_duties_set"],
        row["current_total_price"], current_total_price_set, row["current_total_tax"], current_total_tax_set,
        row["customer_locale"], row["device_id"], discount_codes,
        row["email"], row["estimated_taxes"], row["financial_status"], row["fulfillment_status"], "",
        landing_site, row["landing_site_ref"], row["location_id"], row["merchant_of_record_app_id"], row["name"],
        row["note"], note_attributes, row["number"], row["order_number"], row["order_status_url"],
        row["original_total_duties_set"], payment_gateway_names, row["phone"], row["presentment_currency"],
        row["processed_at"], "", row["reference"], row["referring_site"],
        row["source_identifier"],
        row["source_name"], row["source_url"], row["subtotal_price"], subtotal_price_set, row["tags"], tax_lines,
        row["taxes_included"], row["test"], row["token"], row["total_discounts"], total_discounts_set,
        row["total_line_items_price"], total_line_items_price_set, row["total_outstanding"], row["total_price"],
        total_price_set, total_shipping_price_set, row["total_tax"], total_tax_set,
        row["total_tip_received"], row["total_weight"], row["updated_at"], row["user_id"], billing_address,
        shipping_address, shipping_lines, refunds, payment_terms, line_items, fulfillments, discount_applications, customer)
        sql = "INSERT INTO stageshopify.orders(id, admin_graphql_api_id, app_id, browser_ip, buyer_accepts_marketing," \
              "cancel_reason, cancelled_at, cart_token, checkout_id, checkout_token, client_details, " \
              "currency, closed_at, confirmed, contact_email, created_at, current_subtotal_price, " \
              "current_subtotal_price_set, current_total_discounts, current_total_discounts_set, " \
              "current_total_duties_set, current_total_price, current_total_price_set, current_total_tax, current_total_tax_set, " \
              "customer_locale, device_id, discount_codes, email, estimated_taxes, financial_status, fulfillment_status, gateway," \
              "landing_site, landing_site_ref, location_id, merchant_of_record_app_id, name, note, note_attributes," \
              "number, order_number, order_status_url, original_total_duties_set, payment_gateway_names," \
              "phone, presentment_currency, processed_at , processing_method, reference, referring_site, source_identifier, " \
              "source_name, source_url, subtotal_price, subtotal_price_set, tags, tax_lines, taxes_included, test, token," \
              "total_discounts, total_discounts_set, total_line_items_price, total_line_items_price_set," \
              "total_outstanding, total_price, total_price_set, total_shipping_price_set, total_tax, total_tax_set," \
              "total_tip_received, total_weight, updated_at, user_id, billing_address, shipping_address, shipping_lines," \
              "refunds, payment_terms, line_items, fulfillments, discount_applications, customer)" \
              " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
              " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
              " %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        # print(sql)
        # print(val)
        bdg_cursor.execute(sql, val)  # current_subtotal_price_set
        bdg.commit()
        i = i + 50
        url_ordenes = url_next


# Truncacion de fecha carga
bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".FechaCargaInformacion")

# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga API Shopify'
fecha = time.localtime(time.time())
sql = "INSERT INTO "+EsquemaBD+".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()

bdg.close()
bdg_cursor.close()

# Envio de mail con aviso de termino de ejecución script
envio_mail("Aviso fin ejecución script Shopify en DL")