from datetime import datetime
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

def main():
    # VariablesGlobales
    EsquemaBD = "stagesoftland"

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)

    print("Ejecutando Procesamiento almacenado que actualiza la información para el reporte de cobranza")
    bdg_cursor.execute("call " + EsquemaBD + ".proc_auxiliar;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_auxiliar")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_cobranza_expo;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_cobranza_expo")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_cobranza_nac;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_cobranza_nac")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_dias_calle;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_dias_calle")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_tmp_historia;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_tmp_historia")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_saldo_boletas;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_saldo_boletas")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_planpagos;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_planpagos")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_saldo_proveedores;")
    localtime = time.asctime(time.localtime(time.time()))
    print(localtime)
    print("Fin proceso proc_saldo_proveedores")

    bdg_cursor.execute(" COMMIT; ")
    print("Termino de proceso cobranza")
    exit(1)

if __name__ == "__main__":
    main()