import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_cubas")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_grupo_barrica")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_lotes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallelotes")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_existencia_bodega")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_contratoventa")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallectoventa")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")
    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_ContratoVenta
    i = 0
    kupay_cursor.execute("SELECT Numero, Fecha, CodCliente, Nota, CodCliNac, Saldado, CodigoCto FROM ContratoVenta")
    registrosorigen = kupay_cursor.rowcount
    print("(48) tabla bdg_ContratoVenta")
    for Numero, Fecha, CodCliente, Nota, CodCliNac, Saldado, CodigoCto in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_ContratoVenta(Numero, Fecha, CodCliente, Nota, CodCliNac, " \
                                           "Saldado, CodigoCto) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Numero, Fecha, CodCliente, Nota, CodCliNac, Saldado, CodigoCto)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_ContratoVenta: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ContratoVenta', 'bdg_ContratoVenta', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_detallectoventa
    i = 0
    kupay_cursor.execute("SELECT NumDet, CodProducto, Cantidad, Unidad, Cosecha, Precio, Total,"
                         " Saldo, Litros, SaldoLitros FROM detallectoventa")
    registrosorigen = kupay_cursor.rowcount
    print("(49) tabla bdg_detallectoventa")
    for NumDet, CodProducto, Cantidad, Unidad, Cosecha, Precio, Total, Saldo, \
         Litros, SaldoLitros in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detallectoventa(NumDet, CodProducto, Cantidad, Unidad, " \
                                           "Cosecha, Precio, Total, Saldo, Litros, SaldoLitros) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumDet, CodProducto, Cantidad, Unidad, Cosecha, Precio, Total, Saldo, Litros, SaldoLitros)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_detallectoventa: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallectoventa', 'bdg_detallectoventa', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_lotes
    i = 0
    kupay_cursor.execute("select lote, bin as varbin, botellas, ubicacion, ficha, codcliente, reservado, observacion, "
                         "codenv, estado, temporal, selected, fechares, numsolic, codbod, tipoficha FROM detallelotes")
    registrosorigen = kupay_cursor.rowcount
    print("(50) tabla bdg_detallelotes")
    for lote, varbin, botellas, ubicacion, ficha, codcliente, reservado, observacion, codenv, estado, temporal, \
         selected, fechares, numsolic, codbod, tipoficha in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_detallelotes(lote, bin, botellas, ubicacion, ficha, codcliente, " \
                                           "reservado, observacion, codenv, estado, temporal, selected, fechares, " \
                                           "numsolic, codbod, tipoficha) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s)"
        val = (lote, varbin, botellas, ubicacion, ficha, codcliente, reservado, observacion, codenv, estado, temporal,
               selected, fechares, numsolic, codbod, tipoficha)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_detallelotes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detallelotes', 'bdg_detallelotes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_lotes 51
    i = 0
    kupay_cursor.execute("select ID_Lote,Fecha,NumOrden,CodVino,Qty,Lote,Tipo_Vino,Cuba_Or,CostoMSVSC,Corcho,"
                         "Sellote,CantProc,Estad,CAST(costovino AS float) as CostoVino,Bin,cast(CostoMS as float) "
                         "as CostoMS, CAST(costobot AS float) as CostoBot,Litros,cast(TotalCosto as float) "
                         "as TotalCosto,CAST(valltcuba AS float) as mValLtCuba,CostoProd,Temporal,Ficha,QBodega,"
                         "QtyVSC,LitrosVSC,Temp1,CMSReal,CodKit,CMSFormula,Disponible,tmpExistenciaAl,DifCostoMS,"
                         "CostoBotVSC,TotalCostoVSC,CostoVinoVSC FROM lotes")
    registrosorigen = kupay_cursor.rowcount
    print("(51) tabla bdg_lotes")
    for ID_Lote, Fecha, NumOrden, CodVino, Qty, Lote, Tipo_Vino, Cuba_Or, CostoMSVSC, Corcho, Sellote, CantProc, \
        Estad, CostoVino, Bin, CostoMS, CostoBot, Litros, TotalCosto, ValLtCuba, CostoProd, Temporal, Ficha, QBodega, \
        QtyVSC, LitrosVSC, Temp1, CMSReal, CodKit, CMSFormula, Disponible, tmpExistenciaAl, DifCostoMS, CostoBotVSC, \
         TotalCostoVSC, CostoVinoVSC in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_lotes(ID_Lote,Fecha,NumOrden,CodVino,Qty,Lote,Tipo_Vino,Cuba_Or, " \
                                           "CostoMSVSC,Corcho,Sellote,CantProc,Estad,CostoVino,Bin,CostoMS,CostoBot, " \
                                           "Litros,TotalCosto,ValLtCuba,CostoProd,Temporal,Ficha,QBodega,QtyVSC," \
                                           "LitrosVSC,Temp1,CMSReal,CodKit,CMSFormula,Disponible,tmpExistenciaAl," \
                                           "DifCostoMS,CostoBotVSC,TotalCostoVSC,CostoVinoVSC) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (ID_Lote, Fecha, NumOrden, CodVino, Qty, Lote, Tipo_Vino, Cuba_Or, CostoMSVSC, Corcho, Sellote, CantProc,
               Estad, CostoVino, Bin, CostoMS, CostoBot, Litros, TotalCosto, ValLtCuba, CostoProd, Temporal, Ficha,
               QBodega, QtyVSC, LitrosVSC, Temp1, CMSReal, CodKit, CMSFormula, Disponible, tmpExistenciaAl, DifCostoMS,
               CostoBotVSC, TotalCostoVSC, CostoVinoVSC)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_lotes: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'lotes', 'bdg_lotes', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_grupo_barrica 52
    i = 0
    kupay_cursor.execute("select nNum_Grupo, Capacidad, Existencia,CodVino, Dias,N_Barricas, Botellas,Ubicacion, "
                         "Selected,FechaLlenado, Bodega,Lote FROM grupo_barrica")
    registrosorigen = kupay_cursor.rowcount
    print("(52) tabla bdg_grupo_barrica")
    for nNum_Grupo, Capacidad, Existencia, CodVino, Dias, N_Barricas, Botellas, Ubicacion, \
         Selected, FechaLlenado, Bodega, Lote in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_grupo_barrica(nNum_Grupo, Capacidad, Existencia,CodVino, " \
                                            "Dias,N_Barricas, Botellas,Ubicacion, Selected,FechaLlenado, Bodega,Lote) "\
                                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (nNum_Grupo, Capacidad, Existencia, CodVino, Dias, N_Barricas, Botellas, Ubicacion, Selected,
               FechaLlenado, Bodega, Lote)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_grupo_barrica: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'grupo_barrica', 'bdg_grupo_barrica', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_cubas 53
    i = 0
    kupay_cursor.execute("select Cod_Cuba,Nombre_Cuba, Capacidad, Bodega, Litros_Cosecha, Litros_Dif,Existencia, "
                         "Mezcla, CodVino, Numero, Tipo, F_Alert,Nave, Botellas,Kilos_Cosecha, Cod_Mezcla, "
                         "Bod_Cuba, FactCmLts, ComentCosecha, CodigoCos, FechaLlenado, KilosMacerador, "
                         "KilosRecepcion, KilosGuias, LitrosGota, LitrosPrensa, FRecarga, Termo, Ubicacion, "
                         "NoVigente, FechaEstado, UltUsuarioMod, BodINSMolienda, MontoRecarga FROM cubas")
    registrosorigen = kupay_cursor.rowcount
    print("(53) tabla bdg_cubas")
    for Cod_Cuba, Nombre_Cuba, Capacidad, Bodega, Litros_Cosecha, Litros_Dif, Existencia, Mezcla, CodVino, Numero, \
        Tipo, F_Alert, Nave, Botellas, Kilos_Cosecha, Cod_Mezcla, Bod_Cuba, FactCmLts, ComentCosecha, CodigoCos, \
        FechaLlenado, KilosMacerador, KilosRecepcion, KilosGuias, LitrosGota, LitrosPrensa, FRecarga, Termo, \
         Ubicacion, NoVigente, FechaEstado, UltUsuarioMod, BodINSMolienda, MontoRecarga in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_cubas(Cod_Cuba,Nombre_Cuba, Capacidad, Bodega, Litros_Cosecha, " \
                                             "Litros_Dif,Existencia, Mezcla, CodVino, Numero, Tipo, F_Alert,Nave, " \
                                             "Botellas,Kilos_Cosecha, Cod_Mezcla, Bod_Cuba, FactCmLts, ComentCosecha," \
                                             "CodigoCos, FechaLlenado, KilosMacerador, KilosRecepcion, KilosGuias, " \
                                             "LitrosGota, LitrosPrensa, FRecarga, Termo, Ubicacion, NoVigente, " \
                                             "FechaEstado, UltUsuarioMod, BodINSMolienda, MontoRecarga) " \
                                             "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                             "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                             "%s, %s, %s, %s)"
        val = (Cod_Cuba, Nombre_Cuba, Capacidad, Bodega, Litros_Cosecha, Litros_Dif, Existencia, Mezcla,
                 CodVino, Numero, Tipo, F_Alert, Nave, Botellas, Kilos_Cosecha, Cod_Mezcla, Bod_Cuba, FactCmLts,
                 ComentCosecha, CodigoCos, FechaLlenado, KilosMacerador, KilosRecepcion, KilosGuias, LitrosGota,
                 LitrosPrensa, FRecarga, Termo, Ubicacion, NoVigente, FechaEstado, UltUsuarioMod,
                 BodINSMolienda, MontoRecarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_cubas: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'cubas', 'bdg_cubas', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_existencia_bodega 54
    i = 0
    kupay_cursor.execute("select Cod_UnEmb,CodBod,Botellas,Lote,Temp FROM existencia_bodega")
    registrosorigen = kupay_cursor.rowcount
    print("(54) tabla bdg_existencia_bodega")
    for Cod_UnEmb, CodBod, Botellas, Lote, Temp in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_existencia_bodega (Cod_UnEmb,CodBod,Botellas,Lote,Temp) " \
                                           "VALUES (%s, %s, %s, %s, %s)"
        val = (Cod_UnEmb, CodBod, Botellas, Lote, Temp)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_existencia_bodega: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'existencia_bodega', 'bdg_existencia_bodega', registrosorigen, i, fechacarga)
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

