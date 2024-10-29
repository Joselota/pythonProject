import sys
import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

# VariablesGlobales
EsquemaBD = "stagecomercial"
SistemaOrigen = "stagekupay"
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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".tabla_ventaactual")

    ######################
    # Cargar tablas
    ######################
    # TABLA tabla_ventaactual
    sql = 'SELECT NumFicha, numero, agno, mes, Fecha, Fecha_vencimiento, Tipo_Doc, NumDoc, CodVendedor, Vendedor, ' \
          'CodCliente, Cliente, canal, comuna, Sucursal, codproducto, producto, cosecha, esmuestra, empresa, mercado, ' \
          'Pais, Marca, linea, Calidad, TipoVino, Variedad, color, embalaje, CpCaja, Cajas, Botellas, Litros, Cajas9Lts, ' \
          'Valor, Monedas, `T/CMonedaOrigenaUS$` as TCMonedaOrigenaUSD, `T/CFactura` as TCFactura, MontoMonedaOrigen, ' \
          'Neto, `Neto$` as NetoCLP, IVA, ILA, MontoUSD, `Monto$` as MontoCLP, CuentaIV, CuentaCV, CentroCosto, costoVta,' \
          ' Margen, NFicha, PedidoProduccion, consignatario, ClientePO, FechaPO, FechaIngreso, CodigoTipoVino FROM stagekupay.vista_ventaactual'
    print(sql)
    kupay_cursor.execute(sql)
    print("(61) tabla tabla_ventaactual")
    registrosorigen = kupay_cursor.rowcount
    i = 0
    for NumFicha, numero, agno, mes, Fecha, Fecha_vencimiento, Tipo_Doc, NumDoc, CodVendedor, Vendedor, CodCliente, Cliente, canal, comuna, Sucursal, codproducto, producto, cosecha, esmuestra, empresa, mercado, Pais, Marca, linea, Calidad, TipoVino, Variedad, color, embalaje, CpCaja, Cajas, Botellas, Litros, Cajas9Lts, Valor, Monedas, TCMonedaOrigenaUSD, TCFactura, MontoMonedaOrigen, Neto, NetoCLP, IVA, ILA, MontoUSD, MontoCLP, CuentaIV, CuentaCV, CentroCosto, costoVta, Margen, NFicha, PedidoProduccion, consignatario, ClientePO, FechaPO, FechaIngreso, CodigoTipoVino in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO stagecomercial.tabla_ventaactual (NumFicha, numero, agno, mes, Fecha, Fecha_vencimiento, Tipo_Doc, NumDoc, CodVendedor, Vendedor, CodCliente, Cliente, canal, comuna, Sucursal, codproducto, producto, cosecha, esmuestra, empresa, mercado, Pais, Marca, linea, Calidad, TipoVino, Variedad, color, embalaje, CpCaja, Cajas, Botellas, Litros, Cajas9Lts, Valor, Monedas, T/CMonedaOrigenaUS$, T/CFactura, MontoMonedaOrigen, Neto, Neto$, IVA, ILA, MontoUSD, Monto$, CuentaIV, CuentaCV, CentroCosto, costoVta, Margen, NFicha, PedidoProduccion, consignatario, ClientePO, FechaPO, FechaIngreso, CodigoTipoVino) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        val = (NumFicha, numero, agno, mes, Fecha, Fecha_vencimiento, Tipo_Doc, NumDoc, CodVendedor, Vendedor, CodCliente, Cliente, canal, comuna, Sucursal, codproducto, producto, cosecha, esmuestra, empresa, mercado, Pais, Marca, linea, Calidad, TipoVino, Variedad, color, embalaje, CpCaja, Cajas, Botellas, Litros, Cajas9Lts, Valor, Monedas, TCMonedaOrigenaUSD, TCFactura, MontoMonedaOrigen, Neto, NetoCLP, IVA, ILA, MontoUSD, MontoCLP, CuentaIV, CuentaCV, CentroCosto, costoVta, Margen, NFicha, PedidoProduccion, consignatario, ClientePO, FechaPO, FechaIngreso, CodigoTipoVino)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla tabla_ventaactual: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                       "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'vista_ventaactual', 'tabla_ventaactual', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg.close()
    bdg_cursor.close()
    print("fin cierre de cursores y bases")