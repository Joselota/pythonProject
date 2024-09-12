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
    kupay_cursor.execute('select count(*) as cant from submarca')

    if kupay_cursor.rowcount <= 0:
        print("NO HAY REGISTROS")
        sys.exit(-1)
    else:
        print("Se inicia proceso de carga")
        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso: " + localtime)
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_movim_pedido")
        print("Truncado de tablas en " + EsquemaBD + " ")

        # TABLA bdg_det_producc
        print("(56) tabla movim_pedido")
        kupay_cursor.execute("select CodPro, Fecha, Var, Cal, Cosecha, Cant, ID_MovPedido, NumPed, NumFicha, "
                             "TFicha, CodEst, ID_Stock, CodProducto, QtyIngreso, Unid, Lts_Proc, Tip_Vino, QtyProc, "
                             "Lote, ID_VinoPedido, ID_VinEmbo, QtyEgreso, QtySaldo, CostoLtr, CostoMP, SaldoLtrs, "
                             "BoletinExpo, NumeroDoc, Tipo_Doc, CodLinea, CodBod, HIn, HFin, ToTalHoras, ToTalHH, "
                             "TiempoPerdido, VelocidadTrab, TiempoOperReal, Eficiencia, SaldoPesos, CodOper, "
                             "MontoIngreso, MontoEgreso, CodEnc, Mes, LineaProd, Hora, CodVino, CCosto, NumPersonas, "
                             "TestigosCCalidad, FechaReal, Centralizada, CabOpeNumero, CodKit from movim_pedido")
        registrosorigen = kupay_cursor.rowcount
        i = 0
        for CodPro, Fecha, Var, Cal, Cosecha, Cant, ID_MovPedido, NumPed, NumFicha, TFicha, CodEst, ID_Stock, \
            CodProducto, QtyIngreso, Unid, Lts_Proc, Tip_Vino, QtyProc, Lote, ID_VinoPedido, ID_VinEmbo, QtyEgreso, \
            QtySaldo, CostoLtr, CostoMP, SaldoLtrs, BoletinExpo, NumeroDoc, Tipo_Doc, CodLinea, CodBod, HIn, HFin, \
            ToTalHoras, ToTalHH, TiempoPerdido, VelocidadTrab, TiempoOperReal, Eficiencia, SaldoPesos, CodOper, \
            MontoIngreso, MontoEgreso, CodEnc, Mes, LineaProd, Hora, CodVino, CCosto, NumPersonas, TestigosCCalidad, \
             FechaReal, Centralizada, CabOpeNumero, CodKit in kupay_cursor.fetchall():
            i = i + 1
            if str(CostoLtr) == 'inf':
                CostoLtr = 0
            sql = "INSERT INTO stagekupay.bdg_movim_pedido(CodPro, Fecha, Var, Cal, Cosecha, Cant, ID_MovPedido, " \
                  "NumPed, NumFicha, TFicha, CodEst, ID_Stock, CodProducto, QtyIngreso, Unid, Lts_Proc, Tip_Vino, " \
                  "QtyProc, Lote, ID_VinoPedido, ID_VinEmbo, QtyEgreso, QtySaldo, CostoLtr, CostoMP, SaldoLtrs, " \
                  "BoletinExpo, NumeroDoc, Tipo_Doc, CodLinea, CodBod, HIn, HFin, ToTalHoras, ToTalHH, TiempoPerdido, " \
                  "VelocidadTrab, TiempoOperReal, Eficiencia, SaldoPesos, CodOper, MontoIngreso, MontoEgreso, CodEnc, " \
                  "Mes, LineaProd, Hora, CodVino, CCosto, NumPersonas, TestigosCCalidad, FechaReal, Centralizada, " \
                  "CabOpeNumero, CodKit) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                  "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (CodPro, Fecha, Var, Cal, Cosecha, Cant, ID_MovPedido, NumPed, NumFicha, TFicha, CodEst, ID_Stock,
                   CodProducto, QtyIngreso, Unid, Lts_Proc, Tip_Vino, QtyProc, Lote, ID_VinoPedido, ID_VinEmbo, QtyEgreso,
                   QtySaldo, CostoLtr, CostoMP, SaldoLtrs, BoletinExpo, NumeroDoc, Tipo_Doc, CodLinea, CodBod, HIn, HFin,
                   ToTalHoras, ToTalHH, TiempoPerdido, VelocidadTrab, TiempoOperReal, Eficiencia, SaldoPesos, CodOper,
                   MontoIngreso, MontoEgreso, CodEnc, Mes, LineaProd, Hora, CodVino, CCosto, NumPersonas, TestigosCCalidad,
                   FechaReal, Centralizada, CabOpeNumero, CodKit)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()

        print("Cantidad de registros en la tabla movim_pedido: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, NroRegistroOrigen, " \
              "NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'movim_pedido', 'bdg_movim_pedido', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Muestra fecha y hora actual al finalizar el proceso
        localtime2 = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de finalizacion del proceso")
        print(localtime2)

        # Cierre de cursores y bases de datos
        kupay_cursor.close()
        kupay.close()
        bdg.close()
        bdg_cursor.close()
        print("fin cierre de cursores y bases")

if __name__ == "__main__":
    main()
