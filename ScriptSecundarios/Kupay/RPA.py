import pymysql
import time
import pyodbc as pyodbc
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from DatosConexion.VG import DRIVER, SERVER, DATABASE, UID, PWD
from datetime import datetime


def main():
    # VariablesGlobales
    EsquemaBD = "stagesoftland"
    periodo = 2024
    # Inicializar variables locales
    #now = datetime.now()
    #periodo = now.year
    print(periodo)

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    # Base de datos Softland (Desde donde se leen los datos)
    stringConn: str = (
            'Driver={' + DRIVER + '};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + UID + ';PWD=' + PWD + ';')
    #print(stringConn)
    i = 0
    lista = []
    try:
        conn = pyodbc.connect(stringConn)
        cursor = conn.cursor()

        print("Inicio de proceso de borrado de tablas ")
        bdg_cursor.execute("DELETE FROM stagesoftland.dte_docref where year(FchRef) =" + str(periodo))
        print("Fin de proceso de borrado de tablas ")

        sql = "SELECT RUTEmisor, TipoDTE, Folio, NroLinRef, TpoDocRef, IndGlobal, FolioRef, RUTOtr, " \
              "FchRef, CodRef, RazonRef FROM softland.dte_docref where year(FchRef) =" + str(periodo)
        cursor.execute(sql)

        i = 0
        for RUTEmisor, TipoDTE, Folio, NroLinRef, TpoDocRef, IndGlobal, FolioRef, RUTOtr, FchRef, CodRef, RazonRef in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.dte_docref (RUTEmisor, TipoDTE, Folio, NroLinRef, TpoDocRef, " \
                  "IndGlobal, FolioRef, RUTOtr, FchRef, CodRef, RazonRef )" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (RUTEmisor, TipoDTE, Folio, NroLinRef, TpoDocRef, IndGlobal, FolioRef, RUTOtr, FchRef, CodRef, RazonRef)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tablas cwttdoc cargados a DL :" + str(i))
        # Cerrar la conexión a softland
        cursor.close()
        conn.close()


    except pyodbc.DataError as err:
        print("An exception occurred")
        print(err)


    # Cerrar la conexión del datalake
    bdg_cursor.close()
    bdg.close()


if __name__ == "__main__":
    main()