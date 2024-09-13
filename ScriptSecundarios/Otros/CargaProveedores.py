import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

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

# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

# Generando identificador para proceso de cuadratura
dia = str(100+int(format(fechacarga.day)))
mes = str(100+int(format(fechacarga.month)))
agno = format(fechacarga.year)
Identificador = str(agno) + str(mes[1:]) + str(dia[1:])

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

# Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

print("Consultando disponibilidad de base de datos Kupay")
kupay_cursor.execute('select count(*) as cant from submarca')

if kupay_cursor.rowcount <= 0:
    print("NO HAY REGISTROS")
    sys.exit(-1)
else:
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_proveedores")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_prod_liqu 55
    i = 0
    kupay_cursor.execute("SELECT CodProv, NomProv, Direccion, Comuna, Rut, Dig, Fono, Fax, Ciudad, Giro, PAIS,"
                         "VENDEDOR, email, Rubro, Cod_Giro, cod_comuna, cod_ciudad, cod_pais, "
                         "cod_vendedor, PorIVA, Bloqueado, NoVigente, "
                         "FechaEstado, UltUsuarioMod, CodCondPago FROM proveedores")
    registrosorigen = kupay_cursor.rowcount
    print("(63) tabla bdg_proveedores")
    for CodProv, NomProv, Direccion, Comuna, Rut, Dig, Fono, Fax, Ciudad, Giro, PAIS, VENDEDOR, \
        email, Rubro, Cod_Giro, cod_comuna, cod_ciudad, cod_pais, cod_vendedor, PorIVA, Bloqueado, \
        NoVigente, FechaEstado, UltUsuarioMod, CodCondPago in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_proveedores (CodProv, NomProv, Direccion, Comuna, " \
                                           "Rut, Dig, Fono, Fax, Ciudad, Giro, PAIS, VENDEDOR, email, " \
                                           "Rubro, Cod_Giro, cod_comuna, cod_ciudad, cod_pais, cod_vendedor, " \
                                           "PorIVA, Bloqueado, NoVigente, FechaEstado, UltUsuarioMod, CodCondPago)" \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodProv, NomProv, Direccion, Comuna, Rut, Dig, Fono, Fax, Ciudad, Giro, PAIS, VENDEDOR,
               email, Rubro, Cod_Giro, cod_comuna, cod_ciudad, cod_pais, cod_vendedor, PorIVA, Bloqueado,
               NoVigente, FechaEstado, UltUsuarioMod, CodCondPago)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_proveedores: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'proveedores', 'bdg_proveedores', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)


    # Muestra fecha y hora actual al finalizar el proceso
    localtime2 = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de finalizacion del proceso")
    print(localtime2)

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg_cursor.close()
    bdg.close()
    print("fin cierre de cursores y bases")

# Envio de mail con aviso de termino de ejecución script
envio_mail("Aviso fin ejecución script bdg_proveedores parte1 en DL")