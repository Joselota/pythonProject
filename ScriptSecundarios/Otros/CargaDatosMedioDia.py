import pyodbc
import pymysql
import time
import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage

def envio_mail(v_email_subject):
    email_subject = v_email_subject
    message = EmailMessage()
    message['Subject'] = email_subject
    message['From'] = sender_email
    message['To'] = receiver_email
    message.set_content("Aviso termino de ejecución script")
    server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
    server.ehlo()  # Identify this client to the SMTP server
    server.starttls()  # Secure the SMTP connection
    server.login(sender_email, email_pass)  # Login to email account
    server.send_message(message)  # Send email
    server.quit()  # Close connection to serve

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

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

######################
# Cargar tablas
######################

print("Inicio de proceso de truncado de tablas en " + EsquemaBD + "")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_clientenac")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_clientes")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_submarca")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vinosymostos")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_monedas")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_descuentosvta")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_apelacion")
bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_precios_cliente")

print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

# Base de datos Kupay (Desde donde se leen los datos)
kupay = pyodbc.connect('DSN=kupayC')
kupay_cursor = kupay.cursor()

# TABLA ClienteNac 1
i = 0
kupay_cursor.execute(
    'SELECT CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro, Region, '
    'Telefono, Fax, Comuna, TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario, '
    'Casilla, PrecioObligado, TotalVta, TotalCjs,  CodMon, Canal, Cod_Giro, Cod_Comuna, Cod_Ciudad, '
    'Cod_Pais, Cod_CargoContacto, FPago, NoVigente, Bloqueado, LimiteCredito,  FechaEstado, '
    'UltUsuarioMod, MontoLCreditoRef, AnticipoReq, PorcAnticipo, DiasMora FROM clientenac')
registrosorigen = kupay_cursor.rowcount
print("(1) tabla clientenac")
for CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro, Region, Telefono, \
     Fax, Comuna, TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario, Casilla, \
     PrecioObligado, TotalVta, TotalCjs, CodMon, Canal, Cod_Giro, Cod_Comuna, Cod_Ciudad, Cod_Pais, \
     Cod_CargoContacto, FPago, NoVigente, Bloqueado, LimiteCredito, FechaEstado, UltUsuarioMod, \
     MontoLCreditoRef, AnticipoReq, PorcAnticipo, DiasMora in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_clientenac(CodClienteNac, Nombre, Fantasia, Direccion, " \
                                       "Mercado, CodVendedor, Ciudad, Giro, Region, Telefono, Fax, Comuna, " \
                                       "TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario, " \
                                       "Casilla, PrecioObligado, TotalVta, TotalCjs,  CodMon, Canal, Cod_Giro, " \
                                       "Cod_Comuna, Cod_Ciudad, Cod_Pais, Cod_CargoContacto, FPago, NoVigente, " \
                                       "Bloqueado, LimiteCredito,  FechaEstado, UltUsuarioMod, MontoLCreditoRef, " \
                                       "AnticipoReq, PorcAnticipo, DiasMora) VALUES (%s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro, Region,
           Telefono, Fax, Comuna, TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario,
           Casilla, PrecioObligado, TotalVta, TotalCjs, CodMon, Canal, Cod_Giro, Cod_Comuna, Cod_Ciudad,
           Cod_Pais, Cod_CargoContacto, FPago, NoVigente, Bloqueado, LimiteCredito, FechaEstado,
           UltUsuarioMod, MontoLCreditoRef, AnticipoReq, PorcAnticipo, DiasMora)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla clientenac: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'clientenac', 'bdg_clientenac', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA Clientes 2
i = 0
kupay_cursor.execute(
    'SELECT CodCliente,Nombre,Fantasia,Direccion,Mercado,Idioma,CodVendedor,Estado,Ciudad,Giro,Telefono,Fax,'
    'Agente,Contacto,Incoterm,FormaPagoNom,Pais,ContactoAgente,Recibidor,eMail,Comentario,CodPostal,'
    'PrecioCliente,TotalVta,LimiteCredito,DominioEmail,CodMon,TotalCjs,MarcaCaja,Cod_Giro,NoVigente,'
    'Cod_Ciudad,FechaEstado,CCosto,Bloqueado,OtroPaisVSC,RutContabilizacion,CodigoBaaNVSC,UltUsuarioMod,'
    'CodigoPais,ModVentaSII,MontoLCreditoRef,AnticipoReq,PorcAnticipo,CodClausulaVta, FormaPagoCod, '
    'DiasMora FROM clientes')
registrosorigen = kupay_cursor.rowcount
print("(2) tabla clientes")
for CodCliente, Nombre, Fantasia, Direccion, Mercado, Idioma, CodVendedor, Estado, Ciudad, Giro, Telefono, Fax, \
     Agente, Contacto, Incoterm, FormaPagoNom, Pais, ContactoAgente, Recibidor, eMail, Comentario, CodPostal, \
     PrecioCliente, TotalVta, LimiteCredito, DominioEmail, CodMon, TotalCjs, MarcaCaja, Cod_Giro, NoVigente, \
     Cod_Ciudad, FechaEstado, CCosto, Bloqueado, OtroPaisVSC, RutContabilizacion, CodigoBaaNVSC, UltUsuarioMod, \
     CodigoPais, ModVentaSII, MontoLCreditoRef, AnticipoReq, PorcAnticipo, CodClausulaVta, \
     FormaPagoCod, DiasMora in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_clientes (CodCliente,Nombre,Fantasia,Direccion,Mercado,Idioma," \
                                       "CodVendedor,Estado,Ciudad,Giro,Telefono,Fax,Agente,Contacto,Incoterm," \
                                       "FormaPagoNom,Pais,ContactoAgente,Recibidor,eMail,Comentario,CodPostal," \
                                       "PrecioCliente,TotalVta,LimiteCredito,DominioEmail,CodMon,TotalCjs," \
                                       "MarcaCaja,Cod_Giro,NoVigente,Cod_Ciudad,FechaEstado,CCosto,Bloqueado" \
                                       ",OtroPaisVSC,RutContabilizacion,CodigoBaaNVSC,UltUsuarioMod,CodigoPais," \
                                       "ModVentaSII,MontoLCreditoRef,AnticipoReq,PorcAnticipo,CodClausulaVta, " \
                                       "FormaPagoCod, DiasMora) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                       " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                       " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodCliente, Nombre, Fantasia, Direccion, Mercado, Idioma, CodVendedor, Estado, Ciudad, Giro,
           Telefono, Fax, Agente, Contacto, Incoterm, FormaPagoNom, Pais, ContactoAgente, Recibidor, eMail,
           Comentario, CodPostal, PrecioCliente, TotalVta, LimiteCredito, DominioEmail, CodMon, TotalCjs,
           MarcaCaja, Cod_Giro, NoVigente, Cod_Ciudad, FechaEstado,  CCosto, Bloqueado, OtroPaisVSC,
           RutContabilizacion, CodigoBaaNVSC, UltUsuarioMod, CodigoPais, ModVentaSII, MontoLCreditoRef,
           AnticipoReq, PorcAnticipo, CodClausulaVta, FormaPagoCod, DiasMora)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla clientes: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'clientes', 'bdg_clientes', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA SUBMARCA 3
kupay_cursor.execute(
    'select CodProducto, Producto,CodMarca,CodVar,Cali, CostoSTDProd,Existencia,Id_PT, Moneda,Tipo_vino,Cosecha,'
    'CtaCtble, CodLinea,CCosto,Impuesto,CuentaVtaExport,Etiqueta,CostoStdVino,Comentarios,Temp,NoVigente,Cvalle,'
    'SinArte, CodigoExterno, Temp1,NombreFact,UnidadNegocio,Critico,Nac,TiempoProd,Tolerancia,CodigoArancel,'
    'FechaEstado,KIT,EAN,DUM,Grado,AlturaEt, Capacidad,NroBoletin, UltUsuarioMod,Detalle1,Detalle2 '
    'from submarca order by CodProducto asc')
print("(3) tabla submarca")
registrosorigen = kupay_cursor.rowcount
i = 0
for CodProducto, Producto, CodMarca, CodVar, Cali, CostoSTDProd, Existencia, Id_PT, Moneda, Tipo_vino, \
     Cosecha, CtaCtble, CodLinea, CCosto, Impuesto, CuentaVtaExport, Etiqueta, CostoStdVino, Comentarios, \
     Temp, NoVigente, Cvalle, SinArte, CodigoExterno, Temp1, NombreFact, UnidadNegocio, Critico, Nac, \
     TiempoProd, Tolerancia, CodigoArancel, FechaEstado, KIT, EAN, DUM, Grado, AlturaEt, Capacidad, \
     NroBoletin, UltUsuarioMod, Detalle1, Detalle2 in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_submarca (CodProducto, Producto,CodMarca,CodVar,Cali,  " \
                                       "CostoSTDProd,Existencia,Id_PT, Moneda,Tipo_vino,Cosecha,CtaCtble, " \
                                       "CodLinea,CCosto,Impuesto,CuentaVtaExport,Etiqueta,CostoStdVino," \
                                       "Comentarios,Temp,NoVigente,Cvalle,SinArte, CodigoExterno, Temp1," \
                                       "NombreFact,UnidadNegocio,Critico,Nac,TiempoProd,Tolerancia," \
                                       "CodigoArancel,FechaEstado,KIT,EAN,DUM,Grado,AlturaEt, Capacidad," \
                                       "NroBoletin, UltUsuarioMod,Detalle1,Detalle2) VALUES (%s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                       "%s, %s, %s, %s, %s, %s)"
    val = (CodProducto, Producto, CodMarca, CodVar, Cali, CostoSTDProd, Existencia, Id_PT, Moneda, Tipo_vino, Cosecha,
           CtaCtble, CodLinea, CCosto, Impuesto, CuentaVtaExport, Etiqueta, CostoStdVino, Comentarios, Temp, NoVigente,
           Cvalle, SinArte, CodigoExterno, Temp1, NombreFact, UnidadNegocio, Critico, Nac, TiempoProd, Tolerancia,
           CodigoArancel, FechaEstado, KIT, EAN, DUM, Grado, AlturaEt, Capacidad, NroBoletin, UltUsuarioMod, Detalle1,
           Detalle2)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla submarca: ", i)

# TABLA VINOSYMOSTOS 4
i = 0
kupay_cursor.execute(
    'select TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp, Madera,'
    ' NoVigente, FechaEstado, UltUsuarioMod from vinosymostos')
registrosorigen = kupay_cursor.rowcount
print("(4) tabla vinosymostos")
for TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp, Madera, NoVigente, \
     FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_vinosymostos (TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, " \
                                       "CCosto, CodVar, CaidaDens, DevTemp, Madera, NoVigente, FechaEstado, " \
                                       "UltUsuarioMod) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp, Madera, NoVigente,
           FechaEstado, UltUsuarioMod)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla vinosymostos: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'vinosymostos', 'bdg_vinosymostos', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA MONEDAS 5
i = 0
kupay_cursor.execute(
    'select CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, SOLIdentificador, '
    'SOLTipoTabla from monedas')
print("(5) tabla monedas")
registrosorigen = kupay_cursor.rowcount
for CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, \
     SOLIdentificador, SOLTipoTabla in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_monedas (CodMon, TC, Moneda, Decimales, ABREV, TCPresup, " \
                                       "DescribeIngles, SOLIdentificador, SOLTipoTabla) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, SOLIdentificador, SOLTipoTabla)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla monedas: ", i)

# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                   " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'monedas', 'bdg_monedas', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA descuentosvta 6
i = 0
kupay_cursor.execute(
    'SELECT CodDesc,Concepto,PorcDef,MontoDef,TotFicha,Facturable,Tipo,Abrev,Producto,SobreFOBPdto,'
    'Orden,CtaCtbleVSC FROM descuentosvta')
registrosorigen = kupay_cursor.rowcount
print("(6) tabla descuentosvta")
for CodDesc, Concepto, PorcDef, MontoDef, TotFicha, Facturable, Tipo, Abrev, Producto, SobreFOBPdto, \
     Orden, CtaCtbleVSC in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_descuentosvta(CodDesc,Concepto,PorcDef,MontoDef,TotFicha," \
                                       "Facturable,Tipo,Abrev,Producto,SobreFOBPdto,Orden,CtaCtbleVSC) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (CodDesc, Concepto, PorcDef, MontoDef, TotFicha, Facturable, Tipo,
           Abrev, Producto, SobreFOBPdto, Orden, CtaCtbleVSC)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla descuentosvta: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'descuentosvta', 'bdg_descuentosvta', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA bdg_apelacion 7
i = 0
kupay_cursor.execute("SELECT codigo, apelacion FROM apelacion")
registrosorigen = kupay_cursor.rowcount
print("(7) tabla bdg_apelacion")
for codigo, apelacion in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_apelacion(codigo, apelacion) VALUES (%s, %s)"
    val = (codigo, apelacion)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla bdg_apelacion: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'apelacion', 'bdg_apelacion', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()

# TABLA precios_cliente 8
i = 0
kupay_cursor.execute(
    'select codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, montodes, '
    'fechaestado, ultusuariomod from precios_cliente')
registrosorigen = kupay_cursor.rowcount
print("(8) tabla bdg_precios_cliente")
for codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, \
     montodes, fechaestado, ultusuariomod in kupay_cursor.fetchall():
    i = i + 1
    sql = "INSERT INTO " + EsquemaBD + ".bdg_precios_cliente(codcliente, codproducto, unidad, " \
                                       "codmon, precio, codclientenac, codconcepto, porc, montodes, " \
                                       "fechaestado, ultusuariomod) VALUES " \
                                       "(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    val = (codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, montodes, fechaestado,
           ultusuariomod)
    bdg_cursor.execute(sql, val)
    bdg.commit()
print("Cantidad de registros en la tabla bdg_precios_cliente: ", i)
# Proceso cuadratura de carga
sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                   "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                   "VALUES (%s, %s, %s, %s, %s, %s, %s)"
val = (Identificador, SistemaOrigen, 'precios_cliente', 'bdg_precios_cliente', registrosorigen, i, fechacarga)
bdg_cursor.execute(sql, val)
bdg.commit()


# Truncacion de fecha carga
bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".FechaCargaInformacion WHERE Proceso='P03'")

# Muestra fecha y hora actual al finalizar el proceso
localtime2 = time.asctime(time.localtime(time.time()))
print("Fecha y hora de finalizacion del proceso")
print(localtime2)

# Registro de fecha cargada en base de datos
Proceso = 'P03'
Descripcion = 'Carga medio día'
fecha = datetime.datetime.now()
sql = "INSERT INTO " + EsquemaBD + ".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
# print(sql)
bdg_cursor.execute(sql, val)
bdg.commit()

# Cierre de cursores y bases de datos
kupay_cursor.close()
kupay.close()
bdg.close()
bdg_cursor.close()
print("fin cierre de cursores y bases")


# Envio de mail con aviso de termino de ejecución script
envio_mail("Aviso fin ejecución script Carga datos medio dia en DL")

