from datetime import datetime
import pymysql
import time
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

def main():
    # VariablesGlobales
    EsquemaBD = "stagekupay"
    varLimite = 200
    varPorcCrecimiento = 1.05

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    # ##########################################################################################################
    # En este procedimiento arma la tabla tab_vinos la cual contiene identificador de vino, tipo vino y cosecha
    # Esta tabla es la base para los calculos de controller
    # ##########################################################################################################
    print("Ejecutando Procesamiento almacenado que crea listado de tipos de vinos y cosechas (proc_controller)")
    bdg_cursor.execute("call " + EsquemaBD + ".proc_controller;")

    bdg_cursor.execute("call " + EsquemaBD + ".proc_controller_ventas;")
    print("Termino de proc_controller")

    # ##########################################################################################################
    # limpia tabla cosecha vigente, recordando que esta tabla nos colorea cual es la cosecha
    # que se encuentra vigente para la venta
    # ##########################################################################################################
    print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".cosechavigente")

    # ##########################################################################################################
    # limpia tabla cosecha vigente, recordando que esta tabla nos colorea cual es la cosecha
    # que se encuentra vigente para la venta
    # ##########################################################################################################
    bdg_cursor.execute(
        "update stagekupay.tab_vinos set vigente = 0 , disponible = 0 , NecProd = 0, VentaAnt = 0 , "
        "DispAnt = 0, NecAnt = 0, venta=0, fechaAgotar=null, desde=null, hasta=null, vinocomercial=0, "
        "dispreal=null, semaforo=null, VentaCaja9Lts=0, PanelCaja9Lts=0, PptoCaja9Lts=0 ")
    bdg_cursor.execute("update stagekupay.tab_vinos set tab_vinos.Año = tab_vinos.cosecha")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)


    # ##########################################################################################################
    # Inicio lógica para determinar cosecha vigente
    # ##########################################################################################################
    i = 0
    bdg_cursor.execute("select ID, tipovino, cosecha, "
                       "max(ifnull(cubas,0)+ifnull(embotellado,0)+ifnull(barrica,0)-"
                       "ifnull(pedidos,0)-ifnull(reserva,0)-ifnull(contrato,0)) as disponibilidad "
                       "from stagekupay.vista_enbodega where cosecha >= 18 and cosecha <= 24 and "
                       "length(cosecha)>0 "
                       "group by ID, tipovino, cosecha order by tipovino asc, cosecha asc")
    for ID, tipovino, cosecha, disponibilidad in bdg_cursor.fetchall():
        i = i + 1
        if not disponibilidad == 0:
            sql = "insert into stagekupay.cosechavigente(id, tipo_vino, cosecha, disponibilidad, marca, limite) " \
                  "VALUES (%s, %s, %s, (%s / 9), 0, 0)"
            val = (ID, tipovino, cosecha, disponibilidad)
            bdg_cursor.execute(sql, val)
            bdg.commit()
    print("Cantidad de registros en la tabla cosechavigente: ", i)

    # ##########################################################################################################
    # Actualizando el límite de los vinos  #
    # ##########################################################################################################
    i = 0
    bdg_cursor.execute("Select `Tipo Vino` as TipoVino, `Límite` as limite from stagekupay.limitecosechacontroller ")
    for TipoVino, limite in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.cosechavigente set cosechavigente.limite = %s where tipo_vino = %s "
        val = (limite, TipoVino)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    ## Actualizando el limite de los vinos ##

    # ##########################################################################################################
    # TABLA cosechavigente marcar el registro vigente para venta
    # ##########################################################################################################
    i = 0
    bdg_cursor.execute(
        "select ID, tipo_vino, cosecha, disponibilidad, marca, limite from stagekupay.cosechavigente "
        "order by tipo_vino asc, cosecha asc")
    print("Marcar cosecha vigente")
    for ID, tipo_vino, cosecha, disponibilidad, marca, limite in bdg_cursor.fetchall():
        i = i + 1
        if disponibilidad > limite:
            sql = "update stagekupay.cosechavigente set marca = '1' where id = %s"
            val = ID
            bdg_cursor.execute(sql, val)
            bdg.commit()

    # ##########################################################################################################
    # Marcar cosecha vigente en tab_vinos
    # ##########################################################################################################
    i = 0
    bdg_cursor.execute(
        "select tipo_vino, min(cosecha) as cosecha from stagekupay.cosechavigente where marca = '1' group by tipo_vino")
    for tipo_vino, cosecha in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set vigente = '1' where tipovino = %s and cosecha= %s"
        val = (tipo_vino, cosecha)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Fin logica para determinar cosecha vigente")

    # ##########################################################################################################
    # Inicio de lógica para determinar disponible
    # ##########################################################################################################
    # Sumando Barricas
    i = 0
    bdg_cursor.execute("select id, sum(barricas/9) as disponible from stagekupay.vista_existencia_barrica group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible + %s where id = %s"
        val = (disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Sumando cubas
    i = 0
    bdg_cursor.execute("select id, sum(cubas/9) as disponible from stagekupay.vista_existencia_cubas group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible + %s where id = %s"
        val = (disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Sumando embotellados
    i = 0
    bdg_cursor.execute(
        "select ID, sum(embotellado/9) as disponible from stagekupay.vista_existencia_embotellado group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible + %s where id = %s"
        val = (disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Restando pedidos
    i = 0
    bdg_cursor.execute("select ID, sum(pedidos/9) from stagekupay.vista_existencia_pedido group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible - %s , " \
              "tab_vinos.NecProd = tab_vinos.NecProd + %s where id = %s"
        val = (disponible, disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Restando contrato
    i = 0
    bdg_cursor.execute("select id, sum(litros/9) as disponible  from stagekupay.vista_contrato group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible - %s , " \
              "tab_vinos.NecProd = tab_vinos.NecProd + %s where id = %s"
        val = (disponible, disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Restando reserva
    i = 0
    bdg_cursor.execute("select ID, sum((Botellas*CapacBot/1000)/9) as disponible from stagekupay.vista_reserva group by id")
    for id1, disponible in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.disponible = tab_vinos.disponible - %s , " \
              "tab_vinos.NecProd = tab_vinos.NecProd + %s where id = %s"
        val = (disponible, disponible, id1)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Fin Logica para determinar disponible")

    # Agregando año
    i = 0
    bdg_cursor.execute("select distinct tipovino, factoraño from stagekupay.infocosechas")
    for tipovino, factorano in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos " \
              "   set tab_vinos.Año = RIGHT(((1000+(tab_vinos.cosecha*1)) + IFNULL(%s,0)),2)" \
              " where tipovino = %s"
        val = (factorano, tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Agregando venta
    i = 0
    print("*****************************************************")
    print(" Agregando ventas historicas ")
    bdg_cursor.execute("SELECT right(Agno,2) as Agno, tipovino, VentaCajas9Lts FROM stagekupay.venta_hist_controller")
    for ano, tipovino, cajas9L in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.venta = %s where tipovino = %s and año = %s"
        val = (cajas9L, tipovino, ano)
        print(sql, val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("*****************************************************")
    print("Fin de agregar ventas historicas ")
    """
    # Agregando Ppto2024
    i = 0
    print(" Agregando ppto 2024")
    bdg_cursor.execute(
        "select (sum(cajas9lts)) as cajas9L, Tipo_vino as tipovino from stagekupay.presupuesto2024 "
        "join stagekupay.vista_producto on presupuesto2024.codproducto = vista_producto.codproducto "
        "group by Tipo_vino")
    for cajas9L, tipovino in bdg_cursor.fetchall():
        i = i + 1
        cajas9L = cajas9L
        # varPorcCrecimiento
        sql = "update stagekupay.tab_vinos set tab_vinos.venta = %s where tipovino = %s and año = '24'"
        val = (cajas9L, tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    """
    # Agregando Ppto2025
    """ i = 0
    print(" Agregando ppto 2024")
    bdg_cursor.execute(
        "select (sum(cajas9lts)) as cajas9L, Tipo_vino as tipovino from stagekupay.presupuesto2024 "
        "join stagekupay.vista_producto on presupuesto2024.codproducto = vista_producto.codproducto "
        "group by Tipo_vino")
    for cajas9L, tipovino in bdg_cursor.fetchall():
        i = i + 1
        cajas9L = cajas9L * varPorcCrecimiento * varPorcCrecimiento
        sql = "update stagekupay.tab_vinos set tab_vinos.venta = %s where tipovino = %s and año = '25'"
        val = (cajas9L, tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    """
    print("Agregar venta actual")
    i = 0
    bdg_cursor.execute("SELECT tipovino, VentaCajas9Lts from stagekupay.venta_actu_controller where Agno=2024 and origen='Factura'")
    for Tipo_vino, Cajas9Lts in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set VentaCaja9Lts = %s  where vigente=1  and tipovino = %s "
        val = (Cajas9Lts, Tipo_vino)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    print("Agregar panel actual")
    i = 0
    bdg_cursor.execute("SELECT tipovino, VentaCajas9Lts from stagekupay.venta_actu_controller where Agno=2024 and origen='Ficha'")
    for Tipovino, Cajas9Lts in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set PanelCaja9Lts = %s  where vigente=1 and tipovino = %s "
        val = (Cajas9Lts, Tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("*****************************************************")
    print("Agregar ppto actual en Caja9Lts")
    i = 0
    bdg_cursor.execute(
        "SELECT Tipo_vino, sum(Cajas9Lts) as Cajas9Lts FROM stagekupay.presupuesto2024 "
        " join stagekupay.vista_producto on (vista_producto.CodProducto = presupuesto2024.codproducto) "
        "where length(Tipo_vino)>0 group by Tipo_vino;")
    for Tipovino, Cajas9Lts in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set PptoCaja9Lts = %s  where vigente=1 and tipovino = %s "
        val = (Cajas9Lts, Tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Agregando Disp y Necesidad periodo anterior
    i = 0
    print(" Agregando informacion periodo anterior")

    sql_venta = "select ID, tipovino, cosecha, " \
                "(select sum(ifnull(venta,0)) from stagekupay.tab_vinos as t where t.tipovino = tab_vinos.tipovino and t.cosecha=tab_vinos.cosecha-1) as venta, " \
                "(select sum(ifnull(venta,0)) from stagekupay.tab_vinos as t where t.tipovino = tab_vinos.tipovino and t.cosecha=tab_vinos.cosecha-2) as ventaAnt, " \
                "(select sum(disponible) from stagekupay.tab_vinos as t where t.tipovino = tab_vinos.tipovino and t.cosecha=tab_vinos.cosecha-1) as disponible, " \
                "(select sum(necprod) from stagekupay.tab_vinos as t where t.tipovino = tab_vinos.tipovino and t.cosecha=tab_vinos.cosecha-1) as necesidad " \
                " from stagekupay.tab_vinos where vigente='1'"
    bdg_cursor.execute(sql_venta)
    for ID, tipovino, cosecha, venta, ventaAnt, disponible, necesidad in bdg_cursor.fetchall():
        i = i + 1
        if venta == 0:
            venta=ventaAnt
        if venta is None:
            venta=ventaAnt
        #print(ID, tipovino, cosecha, venta, ventaAnt, disponible, necesidad)
        sql = "update stagekupay.tab_vinos set tab_vinos.ventaant = %s, " \
              "tab_vinos.dispant = %s, tab_vinos.necant = %s  where ID = %s "
        val = (venta, disponible, necesidad, ID)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Calculando fecha
    i = 0
    j = 0
    k = 0
    print("Calculando fecha en que se agotaria el vino")
    v_sql = "select ID, tipovino, cosecha, ifnull(disponible,0), if(ventaAnt=0,venta,ventaAnt) as ventaAnt, " \
            "ifnull(DispAnt,0), ifnull(NecAnt,0), ifnull(necprod,0) as necprod, (SELECT distinct infocosechas.tipovino " \
            "FROM stagekupay.infocosechas where infocosechas.tipovino = tab_vinos.tipovino) as infocosecha " \
            "from stagekupay.tab_vinos where vigente='1' order by tipovino"
    #print(v_sql)
    bdg_cursor.execute(v_sql)
    for ID, tipovino, cosecha, disponible, ventaAnt, DispAnt, NecAnt, necprod, infocosecha in bdg_cursor.fetchall():
        if ventaAnt is None:
            ventaAnt = 0
        if DispAnt is None:
            DispAnt = 0
        if NecAnt is None:
            NecAnt = 0
        if disponible is None:
            disponible = 0
        if necprod is None:
            necprod = 0

        i = i + 1
        if ventaAnt != 0:
            j = (disponible / (ventaAnt / 365)) + (DispAnt / (ventaAnt / 365)) + (NecAnt / (ventaAnt / 365))
        else:
            j = 0
        if infocosecha is None:
            k = 0
        else:
            k = len(str(infocosecha))
        print(ID, tipovino, cosecha, disponible, ventaAnt, DispAnt, NecAnt, necprod, infocosecha, j, k)
        if k != 0:
            sql = "update stagekupay.tab_vinos set tab_vinos.fechaAgotar = DATE_ADD(NOW(), INTERVAL %s DAY) where ID = %s "
            val = (j, ID)
            bdg_cursor.execute(sql, val)
            bdg.commit()

    # Agregando Disp y Necesidad periodo anterior
    i = 0
    dia = 1
    meses = 1
    fecha = ''
    print(" Agregando informacion de los campos desde y hasta")
    bdg_cursor.execute(
        "select distinct tab_vinos.ID, tab_vinos.tipovino, cosecha, Mes, FactorAño, ifnull(FactorCambioCosecha,0)  "
        "from stagekupay.tab_vinos join stagekupay.infocosechas "
        "on (stagekupay.tab_vinos.tipovino = stagekupay.infocosechas.tipovino) where vigente=1")

    for Id, tipovino, cosecha, Mes, FactorAno, FactorCambioCosecha in bdg_cursor.fetchall():
        i = i + 1
        today = datetime.today()
        periodo1 = today.year
        if Mes == 1:
            dia = 31
            meses = '12'
        elif Mes == 7:
            dia = 30
            meses = '06'
        elif Mes == 3:
            dia = 28
            meses = '02'
        elif Mes == 10:
            dia = 30
            meses = '09'
        elif Mes == 6:
            dia = 31
            meses = '05'
        elif Mes == 5:
            dia = 30
            meses = '04'

        if tipovino == 'VIU1' or tipovino == 'VIU2' or tipovino == 'VIU8':
            periodo1 = (2000 + int(cosecha) + int(FactorCambioCosecha))
        else:
            periodo1 = str(int(periodo1) + int(FactorCambioCosecha))
        fecha = str(dia) + '-' + str(meses) + '-' + str(periodo1)
        fecha_dt = datetime.strptime(fecha, '%d-%m-%Y')
        # print(Id, tipovino, cosecha, Mes, FactorAno, FactorCambioCosecha)
        # print(fecha_dt)
        # print("****************")
        if fecha_dt >= today:
            pass
        else:
            fecha = str(dia) + '-' + str(meses) + '-' + str(int(periodo1) + 1)
        fecha_dt = datetime.strptime(fecha, '%d-%m-%Y')
        sql = "update stagekupay.tab_vinos set tab_vinos.hasta = %s, desde=now()  where ID = %s "
        val = (fecha_dt, Id)
        bdg_cursor.execute(sql, val)
        bdg.commit()



    print("Agregando marca de vino comercial")
    i = 0
    bdg_cursor.execute("select tipovino from stagekupay.vinoscomerciales")
    for Tipovino in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set vinocomercial = 1  where tipovino = %s "
        val = Tipovino
        bdg_cursor.execute(sql, val)
        bdg.commit()

    # Incorporando proyecciones 2024
    """i = 0
    print("Incorporando proyecciones 2023 y 2024")
    bdg_cursor.execute("SELECT tipovino, año, Caja9Lts FROM stagekupay.proyeccion where año='24'")
    for tipovino, agno, Caja9Lts in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos set tab_vinos.venta = %s where tipovino = %s and año = %s"
        val = (Caja9Lts, tipovino, agno)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    """
    ##########################################################################################
    # Determinando la disponibilidad real de cada vino que se encuentra vigente para la venta
    ##########################################################################################
    i = 0
    print("Sumatoria de disponibilidad de Saldos")
    bdg_cursor.execute("select tipo_vino, disponibilidad, ID "
                       "  from stagekupay.cosechavigente where ID in "
                       "(select ID from stagekupay.tab_vinos where vigente='1')")
    for tipovino, disponible, ID in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos " \
              "   set tab_vinos.dispReal = %s, " \
              "       tab_vinos.semaforo = %s  " \
              " where tipovino = %s " \
              "   and ID = %s "
        val = (disponible, disponible, tipovino, ID)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    ##############################################################################
    # Incorporando Semaforo
    ##############################################################################
    i = 0
    print(" Determinando la venta del año anterior ")
    bdg_cursor.execute("select sum(ifnull(cajas9lts,0)) as cajas9L, "
                       "       Tipo_vino as tipovino "
                       "  from stagekupay.vista_vta_historica_comp "
                       "  join stagekupay.vista_producto on (vista_vta_historica_comp.codproducto = vista_producto.codproducto) "
                       " where ano = '2023' "
                       " group by Tipo_vino ")
    for cajas9L, tipovino in bdg_cursor.fetchall():
        i = i + 1
        sql = "update stagekupay.tab_vinos " \
              "   set tab_vinos.semaforo = (tab_vinos.semaforo/%s)*12 " \
              " where tipovino = %s " \
              "   and vigente='1'"
        val = (cajas9L, tipovino)
        bdg_cursor.execute(sql, val)
        bdg.commit()

    sql_semaforo = "update stagekupay.tab_vinos   set tab_vinos.semaforo = null where tab_vinos.vigente='1' and venta = 0 "
    bdg_cursor.execute(sql_semaforo)
    bdg.commit()

    print("fin del algoritmo")

    # Cierre de cursores y bases de datos
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")

if __name__ == "__main__":
    main()


