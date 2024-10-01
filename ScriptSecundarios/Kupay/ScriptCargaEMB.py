import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD
from Tools.funciones import f_limpiar

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
        print("Inicio de proceso de truncado de tablas en " + EsquemaBD + " ")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_gd")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_det_emb_prod")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_det_embal")
        print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

        # TABLA bdg_detalle_gd
        kupay_cursor.execute('select NDet_GD, GD_Cant, GD_Unid, GD_Valor, GD_Total, GD_Marca, GD_Vari, GD_Cali, '
                             'GD_Cos, GD_Pedido, GD_Descrip, CodProducto, TipoVino, Tipo, CodVino, Cuba, Existencia, '
                             'TipoX, AptitudGuia, Lote, CBod, TipoMov, CodVinAp, EstCamion, Cuenta, CCosto, CostoVta, '
                             'CBodTr, EsMuestra, TieneLotes, IdDetalleGD, GD_Apelacion, GD_Aptitud, CodigoCuba, '
                             'Mezcla, GradoA, CImputacion from detalle_gd')
        print("(61) tabla bdg_detalle_gd")
        registrosorigen = kupay_cursor.rowcount
        i = 0
        for NDet_GD, GD_Cant, GD_Unid, GD_Valor, GD_Total, GD_Marca, GD_Vari, GD_Cali, GD_Cos, GD_Pedido, GD_Descrip, \
            CodProducto, TipoVino, Tipo, CodVino, Cuba, Existencia, TipoX, AptitudGuia, Lote, CBod, TipoMov, CodVinAp, \
            EstCamion, Cuenta, CCosto, CostoVta, CBodTr, EsMuestra, TieneLotes, IdDetalleGD, GD_Apelacion, GD_Aptitud, \
             CodigoCuba, Mezcla, GradoA, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_gd (NDet_GD, GD_Cant, GD_Unid, GD_Valor, GD_Total, GD_Marca," \
                                               "GD_Vari, GD_Cali, GD_Cos, GD_Pedido, GD_Descrip, CodProducto, TipoVino, " \
                                               "Tipo, CodVino, Cuba, Existencia, TipoX, AptitudGuia, Lote, CBod, TipoMov," \
                                               " CodVinAp, EstCamion, Cuenta, CCosto, CostoVta, CBodTr, EsMuestra, " \
                                               "TieneLotes, IdDetalleGD, GD_Apelacion, GD_Aptitud, CodigoCuba, Mezcla, " \
                                               "GradoA, CImputacion) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (f_limpiar(NDet_GD), f_limpiar(GD_Cant), f_limpiar(GD_Unid), f_limpiar(GD_Valor), f_limpiar(GD_Total),
                   f_limpiar(GD_Marca), f_limpiar(GD_Vari), f_limpiar(GD_Cali), f_limpiar(GD_Cos), f_limpiar(GD_Pedido),
                   f_limpiar(GD_Descrip), f_limpiar(CodProducto), f_limpiar(TipoVino), f_limpiar(Tipo), f_limpiar(CodVino),
                   f_limpiar(Cuba), f_limpiar(Existencia), f_limpiar(TipoX), f_limpiar(AptitudGuia), f_limpiar(Lote),
                   f_limpiar(CBod), f_limpiar(TipoMov), f_limpiar(CodVinAp), f_limpiar(EstCamion), f_limpiar(Cuenta),
                   f_limpiar(CCosto), f_limpiar(CostoVta), f_limpiar(CBodTr), f_limpiar(EsMuestra), f_limpiar(TieneLotes),
                   f_limpiar(IdDetalleGD), f_limpiar(GD_Apelacion), f_limpiar(GD_Aptitud), f_limpiar(CodigoCuba),
                   f_limpiar(Mezcla), f_limpiar(GradoA), f_limpiar(CImputacion))
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_detalle_gd: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalle_gd', 'bdg_detalle_gd', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_det_emb_prod
        i = 0
        kupay_cursor.execute('select ID_MovPedido, CodMat, CantEmb, Merma_Emb, Costo, FStoc, '
                             'TotCosto, ExistBodega from det_emb_prod')
        registrosorigen = kupay_cursor.rowcount
        print("(57) tabla det_emb_prod")
        for ID_MovPedido, CodMat, CantEmb, Merma_Emb, Costo, FStoc, TotCosto, ExistBodega in kupay_cursor.fetchall():
            i = i + 1
            if str(Costo) == 'inf':
                Costo = 0
            elif str(Costo) == '-inf':
                Costo = 0
            if str(TotCosto) == 'inf':
                TotCosto = 0
            elif str(TotCosto) == '-inf':
                TotCosto = 0
            if str(FStoc) == 'inf':
                FStoc = 0
            elif str(FStoc) == '-inf':
                FStoc = 0
            if str(CantEmb) == 'inf':
                CantEmb = 0
            elif str(CantEmb) == '-inf':
                CantEmb = 0
            sql = "INSERT INTO " + EsquemaBD + ".bdg_det_emb_prod (ID_MovPedido, CodMat, CantEmb, Merma_Emb, " \
                                               "Costo, FStoc, TotCosto, ExistBodega) VALUES " \
                                               "(%s, %s, %s, %s, %s, %s, %s, %s) "
            val = (ID_MovPedido, CodMat, CantEmb, Merma_Emb, Costo, FStoc, TotCosto, ExistBodega)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla det_emb_prod: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'det_emb_prod', 'bdg_det_emb_prod', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

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
            print(val)
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
