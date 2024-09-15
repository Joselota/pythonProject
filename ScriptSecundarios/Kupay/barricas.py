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
        print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_enc_rec_uva")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_barricas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detagranel")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vino_cos")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_ccostovinos")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalleODC")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_costovino")
        print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

        print("OK")
        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso" + localtime)

        # TABLA barricas
        i = 0
        kupay_cursor.execute('SELECT GuiaDespacho, Fecha, CodProductor, CodGuia, CodCuartel, CodProdSectCuartel, Cosecha,'
                             ' RecPor, Transp, GradBase, Sector, Comenta, Patente, Hora_In, Hora_Out, Sub_Cuartel,'
                             ' GuiaRecepcion, Contrato, NumViaje, Bodega, TipoCosecha, CtoCos, Dividida, Tipo, gruNeto,'
                             ' gruPropia, gruAdquirida, gruTerceros, gruModeloTransp, gruBruto, gruTara, gruNomChofer, '
                             'gruCodUva FROM enc_rec_uva')
        registrosorigen = kupay_cursor.rowcount
        print("tabla barricas")
        for GuiaDespacho, Fecha, CodProductor, CodGuia, CodCuartel, CodProdSectCuartel, Cosecha, RecPor, Transp, GradBase, Sector, Comenta, Patente, Hora_In, Hora_Out, Sub_Cuartel, GuiaRecepcion, Contrato, NumViaje, Bodega, TipoCosecha, CtoCos, Dividida, Tipo, gruNeto, gruPropia, gruAdquirida, gruTerceros, gruModeloTransp, gruBruto, gruTara, gruNomChofer, gruCodUva in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_enc_rec_uva (GuiaDespacho, Fecha, CodProductor, CodGuia, CodCuartel," \
                                               " CodProdSectCuartel, Cosecha, RecPor, Transp, GradBase, Sector, Comenta," \
                                               " Patente, Hora_In, Hora_Out, Sub_Cuartel, GuiaRecepcion, Contrato," \
                                               " NumViaje, Bodega, TipoCosecha,CtoCos,Dividida,Tipo,gruNeto, gruPropia," \
                                               " gruAdquirida, gruTerceros, gruModeloTransp, gruBruto, gruTara," \
                                               " gruNomChofer, gruCodUva)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" \
                                               ", %s, %s, %s)"
            val = (GuiaDespacho, Fecha, CodProductor, CodGuia, CodCuartel, CodProdSectCuartel, Cosecha, RecPor, Transp, GradBase, Sector, Comenta, Patente, Hora_In, Hora_Out, Sub_Cuartel, GuiaRecepcion, Contrato, NumViaje, Bodega, TipoCosecha, CtoCos, Dividida, Tipo, gruNeto, gruPropia, gruAdquirida, gruTerceros, gruModeloTransp, gruBruto, gruTara, gruNomChofer, gruCodUva)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_enc_rec_uva: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'enc_rec_uva', 'bdg_enc_rec_uva', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()




       # TABLA barricas
        i = 0
        kupay_cursor.execute('SELECT LoteBarricas, CodVino, Selected, Capacidad, Existencia, Codigo_Barrica, Marca, '
                             'Fecha_Compra, Tipo, Color, Madera, Tostado, Estado, Cepillado, Edad, EdadVino, Grupo, '
                             'Orden_Selec, CodNave, Agno, VidaUtil, ValorInicial, ValorResidual, DeprecMes, Eficiencia, '
                             'Bodega, TMade, AgnoRoble, InsertStaves, DepAcumulada, NumActivoFijo, Observaciones, Modelo, '
                             'NoVigente, FechaEstado, UltUsuarioMod FROM barricas')
        registrosorigen = kupay_cursor.rowcount
        print("tabla barricas")
        for LoteBarricas, CodVino, Selected, Capacidad, Existencia, Codigo_Barrica, Marca, Fecha_Compra, Tipo, Color, Madera, Tostado, Estado, Cepillado, Edad, EdadVino, Grupo, Orden_Selec, CodNave, Agno, VidaUtil, ValorInicial, ValorResidual, DeprecMes, Eficiencia, Bodega, TMade, AgnoRoble, InsertStaves, DepAcumulada, NumActivoFijo, Observaciones, Modelo, NoVigente, FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_barricas (LoteBarricas, CodVino, Selected, Capacidad, Existencia, " \
                                               "Codigo_Barrica, Marca, Fecha_Compra, Tipo, Color, Madera, Tostado, Estado, " \
                                               "Cepillado, Edad, EdadVino, Grupo, Orden_Selec, CodNave, Agno, VidaUtil, " \
                                               "ValorInicial, ValorResidual, DeprecMes, Eficiencia, Bodega, TMade, " \
                                               "AgnoRoble, InsertStaves, DepAcumulada, NumActivoFijo, Observaciones, " \
                                               "Modelo, NoVigente, FechaEstado, UltUsuarioMod)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" \
                                               ", %s, %s, %s, %s, %s, %s)"
            val = (LoteBarricas, CodVino, Selected, Capacidad, Existencia, Codigo_Barrica, Marca, Fecha_Compra, Tipo, Color, Madera, Tostado, Estado, Cepillado, Edad, EdadVino, Grupo, Orden_Selec, CodNave, Agno, VidaUtil, ValorInicial, ValorResidual, DeprecMes, Eficiencia, Bodega, TMade, AgnoRoble, InsertStaves, DepAcumulada, NumActivoFijo, Observaciones, Modelo, NoVigente, FechaEstado, UltUsuarioMod)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_barricas: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'barricas', 'bdg_barricas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_detagranel
        i = 0
        kupay_cursor.execute('SELECT Numero, TipoVino, Cosecha, Cant, Ingreso, Egreso, Costo, Monto, '
                             'CtaCtble, CCosto FROM detagranel')
        registrosorigen = kupay_cursor.rowcount
        print("tabla bdg_detagranel")
        for Numero, TipoVino, Cosecha, Cant, Ingreso, Egreso, Costo, Monto, CtaCtble, CCosto in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detagranel (Numero, TipoVino, Cosecha, Cant, Ingreso, Egreso, " \
                                               "Costo, Monto, CtaCtble, CCosto)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Numero, TipoVino, Cosecha, Cant, Ingreso, Egreso, Costo, Monto, CtaCtble, CCosto)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_detagranel: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detagranel', 'bdg_detagranel', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_vino_cos
        i = 0
        kupay_cursor.execute('SELECT TipoVino, cosecha, Litros, CostoP, Cuenta, CCostoVino, CtaMermas, TotalCosto, '
                             'TipoVInoCos, NoVigente, FechaEstado, CCostoCtble, UltUsuarioMod, CodCtaGastos '
                             'FROM vino_cos')
        registrosorigen = kupay_cursor.rowcount
        print("tabla bdg_detagranel")
        for TipoVino, cosecha, Litros, CostoP, Cuenta, CCostoVino, CtaMermas, TotalCosto, TipoVInoCos, NoVigente, FechaEstado, CCostoCtble, UltUsuarioMod, CodCtaGastos in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_vino_cos (TipoVino, cosecha, Litros, CostoP, Cuenta, CCostoVino, " \
                                               "CtaMermas, TotalCosto, TipoVInoCos, NoVigente, FechaEstado, CCostoCtble, " \
                                               "UltUsuarioMod, CodCtaGastos)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (TipoVino, cosecha, Litros, CostoP, Cuenta, CCostoVino, CtaMermas, TotalCosto, TipoVInoCos, NoVigente, FechaEstado, CCostoCtble, UltUsuarioMod, CodCtaGastos)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_vino_cos: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'vino_cos', 'bdg_vino_cos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()


        # TABLA bdg_ccostovinos
        i = 0
        kupay_cursor.execute('SELECT CodigoCC, CentroCosto, Valor, NoVigente, FechaEstado, UltUsuarioMod FROM ccostovinos')
        registrosorigen = kupay_cursor.rowcount
        print("tabla bdg_ccostovinos")
        for CodigoCC, CentroCosto, Valor, NoVigente, FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_ccostovinos (CodigoCC, CentroCosto, Valor, NoVigente, " \
                                               "FechaEstado, UltUsuarioMod)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s)"
            val = (CodigoCC, CentroCosto, Valor, NoVigente, FechaEstado, UltUsuarioMod)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_ccostovinos: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'ccostovinos', 'bdg_ccostovinos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_detalleodc
        i = 0
        kupay_cursor.execute('SELECT NumODC, Cantidad, Describe, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, '
                             'Stock, SaldoODC, Unidad, FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, '
                             'ValorImpuesto, TotalImpuesto, NLinea, NLineaSC FROM detalleodc')
        registrosorigen = kupay_cursor.rowcount
        print("tabla bdg_detalleodc")

        for NumODC, Cantidad, Describe_det, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, ValorImpuesto, TotalImpuesto, NLinea, NLineaSC in kupay_cursor.fetchall():
            i = i + 1
            if str(PorcDcto) == '-inf':
                PorcDcto = 0
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalleodc (NumODC, Cantidad, Describe_det, Valor, Sub_Total, " \
                                               "Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, " \
                                               "FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, " \
                                               "ValorImpuesto, TotalImpuesto, NLinea, NLineaSC)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s)"
            val = (NumODC, Cantidad, Describe_det, Valor, Sub_Total, Codigo, Tipo, Recibido, PorRecibir, Stock, SaldoODC, Unidad, FechaRecepcion, PorcDcto, ValorMinimo, NFichaEx, CodImpuesto, ValorImpuesto, TotalImpuesto, NLinea, NLineaSC)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_detalleodc: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalleodc', 'bdg_detalleodc', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()



        # TABLA costovino
        i = 0
        kupay_cursor.execute('SELECT CodVino, ID_Costo, CostoGranel, CostoTotal, CostoProd, Factor FROM costovino')
        registrosorigen = kupay_cursor.rowcount
        print("tabla bdg_costovino")
        for CodVino, ID_Costo, CostoGranel, CostoTotal, CostoProd, Factor in kupay_cursor.fetchall():
            i = i + 1
            if str(CostoGranel) == 'inf':
                CostoGranel = 0
            if str(CostoGranel) == '-inf':
                CostoGranel = 0
            if str(CostoTotal) == 'inf':
                CostoTotal = 0
            if str(CostoTotal) == '-inf':
                CostoTotal = 0
            if str(CostoProd) == 'inf':
                CostoProd = 0
            if str(CostoProd) == '-inf':
                CostoProd = 0
            sql = "INSERT INTO " + EsquemaBD + ".bdg_costovino (CodVino, ID_Costo, CostoGranel, CostoTotal, " \
                                               "CostoProd, Factor)" \
                                               " VALUES (%s, %s, %s, %s, %s, %s)"
            val = (CodVino, ID_Costo, CostoGranel, CostoTotal, CostoProd, Factor)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla costovino: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'costovino', 'bdg_costovino', registrosorigen, i, fechacarga)
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