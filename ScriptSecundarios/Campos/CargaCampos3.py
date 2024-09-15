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
    message['To'] = 'igonzalez@viumanent.cl'
    message.set_content("Aviso termino de ejecución script")
    server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
    server.ehlo()  # Identify this client to the SMTP server
    server.starttls()  # Secure the SMTP connection
    server.login(sender_email, email_pass)  # Login to email account
    server.send_message(message)  # Send email
    server.quit()  # Close connection to serve

def main():
    # VariablesGlobales
    EsquemaBD = "stagecampos"
    SistemaOrigen = "Campos"
    fechacarga = datetime.datetime.now()

    # Generando identificador para proceso de cuadratura
    dia = str(100+int(format(fechacarga.day)))
    mes = str(100+int(format(fechacarga.month)))
    agno = format(fechacarga.year)
    Identificador = str(agno) + str(mes[1:]) + str(dia[1:])

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")
    bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".costolab where year(Fecha)=2024")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Base de datos Kupay (Desde donde se leen los datos)
    Campos = pyodbc.connect('DSN=CamposV3')
    campos_cursor = Campos.cursor()

    # TABLA costolab
    i = 0
    campos_cursor.execute('SELECT CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, '
                          'Costo, IdActividad FROM costolab  where year(Fecha)=2024')
    registrosorigen = campos_cursor.rowcount
    print("(47) tabla costolab")
    print(registrosorigen)
    for CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad in campos_cursor.fetchall():
        i = i + 1
        if str(Costo) == 'inf':
            Costo = 0
        print(CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad)
        sql = "INSERT INTO " + EsquemaBD + ".costolab(CodMovDia, CodLabor, CodCuartel, CodPer, " \
                                           "Fecha, Folio, Costo, IdActividad) " \
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (CodMovDia, CodLabor, CodCuartel, CodPer, Fecha, Folio, Costo, IdActividad)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla costolab: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'costolab', 'costolab', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()


    # Cierre de cursores y bases de datos
    campos_cursor.close()
    Campos.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas en Campos 3/9")


if __name__ == "__main__":
    main()