import sys
import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD
from Tools.funciones import f_limpiar

def main():
    # VariablesGlobales
    EsquemaBD = "stagekupay"
    SistemaOrigen = "Kupay"

    fechacarga = datetime.now()
    print(fechacarga)

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
        print("Se inicia proceso de carga")
        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso: " + localtime)
        print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_det_embal")
        print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

        # TABLA bdg_det_embal
        kupay_cursor.execute('select IDEmb, CodMat, Cant, CodFam, QtyProc, Costo, FReservaMS from det_embal')
        print("(62) tabla bdg_det_embal")
        registrosorigen = kupay_cursor.rowcount
        i = 0
        for IDEmb, CodMat, Cant, CodFam, QtyProc, Costo, FReservaMS in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_det_embal (IDEmb, CodMat, Cant, CodFam, " \
                                               "QtyProc, Costo, FReservaMS) VALUES (%s, %s, %s, %s, %s, %s, %s)"
            val = (IDEmb, CodMat, Cant, CodFam, QtyProc, Costo, FReservaMS)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_det_embal: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'det_embal', 'bdg_det_embal', registrosorigen, i, fechacarga)
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

if __name__ == "__main__":
    main()