import sys
import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

# VariablesGlobales
EsquemaBD = "stagekupay"
SistemaOrigen = "Kupay"
fechacarga = datetime.now()

# Inicializar variables locales
now = datetime.now()
AgnoACarga = now.year
MesDeCarga = now.month
AgnoAnteriorCarga = AgnoACarga - 1

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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_pedido")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_pedido")
    bdg_cursor.execute("COMMIT;")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + "")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

     # TABLA bdg_pedido 46
    i = 0
    kupay_cursor.execute(
        "select NumPedido,Fecha,Fecha_entrega,Codcli,NumFicha,Tficha,PorAvan,Coment,Estado,QtyPEdido,Ref,Despacho,"
        "TipoPedido,Modificado,OrdenCompra,Confirmado,Cliente,NoStd,Nulo,CodEncargado,MarcaCaja,Pallets,"
        "Fecha_requerida,FechaTermino,PtoDestino,Embarcador,FechaSTK,ETDVigna,TotBotellas,NumPedidoSAP,"
        "MaterialPOS FROM pedido")
    registrosorigen = kupay_cursor.rowcount
    print("(46) tabla bdg_pedido")
    for NumPedido, Fecha, Fecha_entrega, Codcli, NumFicha, Tficha, PorAvan, Coment, Estado, QtyPEdido, Ref, \
        Despacho, TipoPedido, Modificado, OrdenCompra, Confirmado, Cliente, NoStd, Nulo, CodEncargado, MarcaCaja, \
        Pallets, Fecha_requerida, FechaTermino, PtoDestino, Embarcador, FechaSTK, ETDVigna, TotBotellas, \
         NumPedidoSAP, MaterialPOS in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_pedido (NumPedido,Fecha,Fecha_entrega,Codcli,NumFicha,Tficha," \
                                           "PorAvan,Coment,Estado,QtyPEdido,Ref,Despacho,TipoPedido,Modificado," \
                                           "OrdenCompra,Confirmado,Cliente,NoStd,Nulo,CodEncargado,MarcaCaja," \
                                           "Pallets,Fecha_requerida,FechaTermino,PtoDestino,Embarcador,FechaSTK," \
                                           "ETDVigna,TotBotellas,NumPedidoSAP,MaterialPOS) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumPedido, Fecha, Fecha_entrega, Codcli, NumFicha, Tficha, PorAvan, Coment, Estado, QtyPEdido, Ref,
               Despacho, TipoPedido, Modificado, OrdenCompra, Confirmado, Cliente, NoStd, Nulo, CodEncargado,
               MarcaCaja, Pallets, Fecha_requerida, FechaTermino, PtoDestino, Embarcador, FechaSTK, ETDVigna,
               TotBotellas, NumPedidoSAP, MaterialPOS)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_pedido: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'pedido', 'bdg_pedido', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_detalle_pedido 47
    i = 0
    kupay_cursor.execute(
        "select NumPedido,CVar,CCal,Cosecha,Cant,Unid,CMarc,ID_Pack,Nom_Marc,MermaVin,CantProc,Tip_Vino,Lts,"
        "SaldoLts,CodProducto,Botellas,Botella,Corcho,Capacidad,MoStd,FReservaCAS,FecPrograma,FechaEtiq,"
        "LineaEmbotelado,Selected,QCasillero,ConVale,SinArte,CodPro,FechaProduccion,Boletin,Kilos,LtsBoletin,"
        "TipoEtiqueta,CasilleroAsig,CodCasillero,HoraIn,HoraFin,Programado,Grado,CodKit,FReservaMS,Saldado,"
        "Observaciones FROM detalle_pedido ")
    registrosorigen = kupay_cursor.rowcount
    print("(47) tabla bdg_detalle_pedido")
    for NumPedido, CVar, CCal, Cosecha, Cant, Unid, CMarc, ID_Pack, Nom_Marc, MermaVin, CantProc, Tip_Vino, Lts, \
        SaldoLts, CodProducto, Botellas, Botella, Corcho, Capacidad, MoStd, FReservaCAS, FecPrograma, FechaEtiq, \
        LineaEmbotelado, Selected, QCasillero, ConVale, SinArte, CodPro, FechaProduccion, Boletin, Kilos, \
        LtsBoletin, TipoEtiqueta, CasilleroAsig, CodCasillero, HoraIn, HoraFin, Programado, Grado, CodKit, \
         FReservaMS, Saldado, Observaciones in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_pedido (NumPedido,CVar,CCal,Cosecha,Cant,Unid," \
                                           "CMarc,ID_Pack,Nom_Marc,MermaVin,CantProc,Tip_Vino,Lts,SaldoLts," \
                                           "CodProducto,Botellas,Botella,Corcho,Capacidad,MoStd,FReservaCAS," \
                                           "FecPrograma,FechaEtiq,LineaEmbotelado,Selected,QCasillero,ConVale," \
                                           "SinArte,CodPro,FechaProduccion,Boletin,Kilos,LtsBoletin,TipoEtiqueta," \
                                           "CasilleroAsig,CodCasillero,HoraIn,HoraFin,Programado,Grado,CodKit," \
                                           "FReservaMS,Saldado,Observaciones) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                           " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumPedido, CVar, CCal, Cosecha, Cant, Unid, CMarc, ID_Pack, Nom_Marc, MermaVin, CantProc,
               Tip_Vino, Lts, SaldoLts, CodProducto, Botellas, Botella, Corcho, Capacidad, MoStd, FReservaCAS,
               FecPrograma, FechaEtiq, LineaEmbotelado, Selected, QCasillero, ConVale, SinArte, CodPro,
               FechaProduccion, Boletin, Kilos, LtsBoletin, TipoEtiqueta, CasilleroAsig, CodCasillero, HoraIn,
               HoraFin, Programado, Grado, CodKit, FReservaMS, Saldado, Observaciones)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_detalle_pedido: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalle_pedido', 'bdg_detalle_pedido', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Muestra fecha y hora actual al finalizar el proceso
    localtime2 = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de finalizacion del proceso")
    print(localtime2)

    # Registro de fecha cargada en base de datos
    Proceso = 'P02'
    Descripcion = 'Carga Balance'
    fecha = time.localtime(time.time())
    sql = "INSERT INTO FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
    val = (Proceso, Descripcion, fecha)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")