import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD


def main():
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
    kupay_cursor.execute("select count(*) as cant from submarca")
    print(kupay_cursor.rowcount)
    if kupay_cursor.rowcount <= 0:
        print("NO HAY REGISTROS")
        sys.exit(-1)
    else:
        print("OK")
        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso" + localtime)
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalleconsvinos")
        print("Fin del proceso de truncado de tablas")

       # TABLA detalleconsvinos
        i = 0
        kupay_cursor.execute('SELECT Numero, TipoVino, Cosecha, Cant, Lote, Estado, CodProducto, IDDetConsVinos, Botella, Corcho, '
                             'Capacidad, CodLinea, Existencia, CodBodV, Costo, CodCal, LoteDestino, NumFicha, TFicha, Unid, Bin, '
                             'CtaGastos, CCosto, AjusteCostoMS, AjusteCostoVino, Ubicacion, Receta, NroINV, Centralizada, '
                             'CabOpeNumero, CostoLitro, CostoProd FROM detalleconsvinos')
        registrosorigen = kupay_cursor.rowcount
        print("(56) tabla detalleconsvinos")
        for Numero, TipoVino, Cosecha, Cant, Lote, Estado, CodProducto, IDDetConsVinos, Botella, Corcho, Capacidad, CodLinea, Existencia, CodBodV, \
            Costo, CodCal, LoteDestino, NumFicha, TFicha, Unid, Bin, CtaGastos, CCosto, AjusteCostoMS, AjusteCostoVino, Ubicacion, Receta, NroINV, \
            Centralizada, CabOpeNumero, CostoLitro, CostoProd in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalleconsvinos (Numero, TipoVino, Cosecha, Cant, Lote, Estado, CodProducto, " \
                                               "IDDetConsVinos, Botella, Corcho, Capacidad, CodLinea, Existencia, CodBodV, Costo, " \
                                               "CodCal, LoteDestino, NumFicha, TFicha, Unid, Bin, CtaGastos, CCosto, AjusteCostoMS, " \
                                               "AjusteCostoVino, Ubicacion, Receta, NroINV, Centralizada, CabOpeNumero, CostoLitro, " \
                                               "CostoProd) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            val = (Numero, TipoVino, Cosecha, Cant, Lote, Estado, CodProducto, IDDetConsVinos, Botella, Corcho, Capacidad, CodLinea, Existencia,
                   CodBodV, Costo, CodCal, LoteDestino, NumFicha, TFicha, Unid, Bin, CtaGastos, CCosto, AjusteCostoMS, AjusteCostoVino, Ubicacion,
                   Receta, NroINV, Centralizada, CabOpeNumero, CostoLitro, CostoProd)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla deta_pacag: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalleconsvinos', 'bdg_detalleconsvinos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()


        # Cierre de cursores y bases de datos
        kupay_cursor.close()
        kupay.close()
        bdg.close()
        bdg_cursor.close()
        print("fin cierre de cursores y bases")

if __name__ == "__main__":
    main()