import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage


# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.datetime.now()

# Inicializar variables locales
AgnoACarga = 2020

# Generando identificador para proceso de cuadratura
dia = str(100 + int(format(fechacarga.day)))
mes = str(100 + int(format(fechacarga.month)))
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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_prod_liqu")
    # bdg_cursor.execute("TRUNCATE FROM " + EsquemaBD + ".bdg_prod_liqu where year(fechaIngMader)>=" + str(AgnoACarga))
    bdg_cursor.execute(" COMMIT; ")
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
    kupay_cursor.execute("select CodVino, TipoVino, calidad, Cosecha, Codigo, Estado, Aptitud, Remontajes, Falert, "
                         "NumAnalisis, cast(totalcosto as float) as totalcosto, Boletin, FPA, Observacion, Trasiegos, "
                         "Reserva, FechDispon, CodApela, FechaIngMader, FechaOutMad FROM prod_liqu ")
    registrosorigen = kupay_cursor.rowcount
    print("(55) tabla bdg_prod_liqu")
    for CodVino, TipoVino, calidad, Cosecha, Codigo, Estado, Aptitud, Remontajes, Falert, NumAnalisis, TotalCosto, \
        Boletin, FPA, Observacion, Trasiegos, Reserva, FechDispon, CodApela, FechaIngMader, \
        FechaOutMad in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_prod_liqu(CodVino, TipoVino, calidad, Cosecha, Codigo, Estado, " \
                                           "Aptitud, Remontajes, Falert, NumAnalisis, TotalCosto, Boletin, FPA, " \
                                           "Observacion, Trasiegos, Reserva, FechDispon, CodApela, FechaIngMader, " \
                                           "FechaOutMad) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodVino, TipoVino, calidad, Cosecha, Codigo, Estado, Aptitud, Remontajes, Falert, NumAnalisis,
               TotalCosto, Boletin, FPA, Observacion, Trasiegos, Reserva, FechDispon, CodApela, FechaIngMader,
               FechaOutMad)
        bdg_cursor.execute(sql, val)
        print(val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_prod_liqu: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'prod_liqu', 'bdg_prod_liqu', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

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

exit(1)
