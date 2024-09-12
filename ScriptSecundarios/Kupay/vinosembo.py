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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vinos_embo")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vinos")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_bodegamat")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_pacaging")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_embalaje")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_ordencompra")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    # TABLA ordencompra 38
    i = 0
    kupay_cursor.execute(
        "SELECT numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, "
        "nomprov, codcc, codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, "
        "estado, fechasolic, prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, "
        "fechacotizacion, porcdcto, codemp, codcondpago, fichaexpo, observasolic,fechaapbsol, fechaestpago, "
        "desde_campos, enviadaf700, origencabid FROM ordencompra")
    registrosorigen = kupay_cursor.rowcount
    print("(38) tabla bdg_ordencompra")
    for numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, nomprov, codcc, \
        codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, estado, fechasolic, \
        prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, fechacotizacion, porcdcto, codemp, \
        codcondpago, fichaexpo, observasolic, fechaapbsol, fechaestpago, desde_campos, enviadaf700, \
         origencabid in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_ordencompra(numodc, fechaodc, fechaentrega, codprov, nota, neto, " \
                                           "iva, total, codemite, lugarentrega, nomprov, codcc, codenc, plazo, " \
                                           "exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, " \
                                           "estado, fechasolic, prioridad, vb2, autoriza2, recibe, nomrecibe, " \
                                           "numsolicitud, cotizacion, fechacotizacion, porcdcto, codemp, codcondpago," \
                                           "fichaexpo, observasolic,fechaapbsol, fechaestpago, desde_campos, " \
                                           "enviadaf700, origencabid) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        val = (numodc, fechaodc, fechaentrega, codprov, nota, neto, iva, total, codemite, lugarentrega, nomprov, codcc,
               codenc, plazo, exenta, codmon, vb, autoriza, solicita, descuento, atencion, nomemite, estado, fechasolic,
               prioridad, vb2, autoriza2, recibe, nomrecibe, numsolicitud, cotizacion, fechacotizacion, porcdcto,
               codemp, codcondpago, fichaexpo, observasolic, fechaapbsol, fechaestpago, desde_campos,
               enviadaf700, origencabid)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_ordencompra: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'ordencompra', 'bdg_ordencompra', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()



    # TABLA bdg_vinos 40
    i = 0
    kupay_cursor.execute(
        "select Codigo,NomVino,CodVariedad,CodCalidad,COSECHA,Cant,Temp,NoVigente,FechaEstado,UltUsuarioMod from vinos")
    registrosorigen = kupay_cursor.rowcount
    print("(40) tabla bdg_vinos")
    for Codigo, NomVino, CodVariedad, CodCalidad, COSECHA, Cant, Temp, NoVigente, FechaEstado, \
         UltUsuarioMod in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_vinos(Codigo,NomVino,CodVariedad,CodCalidad,COSECHA," \
                                           "Cant,Temp,NoVigente,FechaEstado,UltUsuarioMod) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        val = (Codigo, NomVino, CodVariedad, CodCalidad, COSECHA, Cant, Temp, NoVigente, FechaEstado, UltUsuarioMod)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_vinos: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'vinos', 'bdg_vinos', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_bodegamat 41
    i = 0
    kupay_cursor.execute("select codmat, codbod, existencia, codins from bodegamat")
    registrosorigen = kupay_cursor.rowcount
    print("(41) tabla bdg_bodegamat")
    for codmat, codbod, existencia, codins in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_bodegamat(codmat, codbod, existencia, codins) VALUES (%s, %s, %s, %s) "
        val = (codmat, codbod, existencia, codins)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_bodegamat: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'bodegamat', 'bdg_bodegamat', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA pacaging 42
    i = 0
    kupay_cursor.execute(
        "SELECT codmat, nommat, codproducto, codpac, nitem, qtyun, costostd, ultprecio, cosecha, grado FROM pacaging")
    registrosorigen = kupay_cursor.rowcount
    print("(42) tabla bdg_pacaging")
    for codmat, nommat, codproducto, codpac, nitem, qtyun, costostd, ultprecio, \
         cosecha, grado in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_pacaging(codmat, nommat, codproducto, codpac, " \
                                           "nitem, qtyun, costostd, ultprecio, cosecha, grado) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
        val = (codmat, nommat, codproducto, codpac, nitem, qtyun, costostd, ultprecio, cosecha, grado)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_pacaging: ", i)

    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'pacaging', 'bdg_pacaging', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA pacaging 43
    i = 0
    kupay_cursor.execute("SELECT codproducto, embalaje, cant, codmat, codvar, cmar, precio, peso, "
                         "ultprecio FROM embalaje")
    registrosorigen = kupay_cursor.rowcount
    print("(43) tabla bdg_embalaje")
    for codproducto, embalaje, cant, codmat, codvar, cmar, precio, peso, ultprecio in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_embalaje(codproducto, embalaje, cant, codmat, codvar, cmar, " \
                                           "precio, peso, ultprecio) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
        val = (codproducto, embalaje, cant, codmat, codvar, cmar, precio, peso, ultprecio)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_embalaje: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'embalaje', 'bdg_embalaje', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_vinos_embo 39
    i = 0
    kupay_cursor.execute(
        "select cod_unemb, cosecha, vari, cali, estad, cant, lote, codigo_vino, lts_emb, tipovino, dibujo, "
        "codproducto, cast(costobotella as float) as costobotella, costoltr, botella, corcho, capacidad, "
        "codmarc, cuenta, codlinea, centrocosto, costototal, cast(costoms as float) as costoms, fechacreacion, "
        "tmpexistenciaal, codcasillero, disponible, costoprod, codctagastos from vinos_embo")
    registrosorigen = kupay_cursor.rowcount
    print("(39) tabla bdg_vinos_embo")
    for cod_unemb, cosecha, vari, cali, estad, cant, lote, codigo_vino, lts_emb, tipovino, dibujo, codproducto, \
        costobotella, costoltr, botella, corcho, capacidad, codmarc, cuenta, codlinea, centrocosto, costototal, \
        costoms, fechacreacion, tmpexistenciaal, codcasillero, disponible, costoprod, \
        codctagastos in kupay_cursor.fetchall():
        i = i + 1
        if str(costoltr) == 'inf':
            costoltr = 0
        if str(costototal) == 'inf':
            costototal = 0
        sql = "INSERT INTO " + EsquemaBD + ".bdg_vinos_embo(cod_unemb, cosecha, vari, cali, estad, cant, lote, " \
                                           "codigo_vino, lts_emb, tipovino, dibujo, codproducto, costobotella, " \
                                           "costoltr, botella, corcho, capacidad, codmarc, cuenta, codlinea, " \
                                           "centrocosto, costototal, costoms, fechacreacion, tmpexistenciaal, " \
                                           "codcasillero, disponible, costoprod, codctagastos) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                           "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
        val = (cod_unemb, cosecha, vari, cali, estad, cant, lote, codigo_vino, lts_emb, tipovino, dibujo, codproducto,
               costobotella, costoltr, botella, corcho, capacidad, codmarc, cuenta, codlinea, centrocosto, costototal,
               costoms, fechacreacion, tmpexistenciaal, codcasillero, disponible, costoprod, codctagastos)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_vinos_embo: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                       " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'vinos_embo', 'bdg_vinos_embo', registrosorigen, i, fechacarga)
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
