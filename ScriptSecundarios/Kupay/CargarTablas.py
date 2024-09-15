import math
import sys
import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, email_smtp  # , receiver_email
import smtplib
from email.message import EmailMessage


def envio_mail(v_email_subject):
    email_subject = v_email_subject
    message = EmailMessage()
    message['Subject'] = email_subject
    message['From'] = sender_email
    message['To'] = 'igonzalez@viumanent.cl'
    message.set_content("Aviso termino de ejecución script")
    server = smtplib.SMTP(email_smtp, 587)  # Set smtp server and port
    server.ehlo()  # Identify this client to the SMTP server
    server.starttls()  # Secure the SMTP connection
    server.login(sender_email, email_pass)  # Login to email account
    server.send_message(message)  # Send email
    server.quit()  # Close connection to serve

def main():
    # VariablesGlobales
    EsquemaBD = "stagekupay"
    SistemaOrigen = "Kupay"
    fechacarga = datetime.now()

    # Inicializar variables locales
    now = datetime.now()
    AgnoACarga = now.year
    MesDeCarga = now.month
    AgnoAnteriorCarga = MesDeCarga - 1
    print("Periodo de carga : " + str(AgnoACarga) + str(MesDeCarga))

    # Generando identificador para proceso de cuadratura
    dia = str(100 + int(format(fechacarga.day)))
    mes = str(100 + int(format(fechacarga.month)))
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
        return
    else:
        print("OK")
        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso" + localtime)
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_submarca")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_empresas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_monedas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vendedor")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_marcas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_calidad")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_lineas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_variedad")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vinosymostos")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_estadosventa")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_clientenac")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_clientes")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_ficha_nac")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_ficha_exp")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_insembarque")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallemu_fexp")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallemu_fnac")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_fac")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_facturas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_factura_expo")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_deta_facexp")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_ficha")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_fichanac")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_deta_embfactn")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_descuentosvta")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_materiales")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_notacredito")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallenc")
        # bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_monvalordia")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_notacreditoexp")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detallencexp")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_fami_mat")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_precios_cliente")
        # bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_movi_insumos")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_bodegas")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_insumos")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_condicionpago")
        bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".bdg_monvalordia where year(fecha)=" + str(AgnoACarga))
        bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".bdg_movi_insumos where year(fecha)>=" + str(AgnoAnteriorCarga))
    #    bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".bdg_detalle_fac "
    #                    "where numero in (select distinct numero from "
    #                       + EsquemaBD + ".bdg_facturas where year(fecha)= " + str(AgnoACarga) + ")")
    #    bdg_cursor.execute("DELETE FROM " + EsquemaBD + ".bdg_deta_facexp "
    #                    "where NumeroFactExp in (select distinct NumeroFactExp from "
    #                       + EsquemaBD + ".bdg_factura_expo where year(fecha)= " + str(AgnoACarga) + ")")

        bdg_cursor.execute(" COMMIT; ")
        print("Fin del proceso de truncado de tablas")

        ######################
        # Cargar tablas
        ######################

        # TABLA SUBMARCA 1
        kupay_cursor.execute('select CodProducto, Producto,CodMarca,CodVar,Cali, CostoSTDProd,Existencia,'
                             'Id_PT, Moneda,Tipo_vino,Cosecha,CtaCtble, CodLinea,CCosto,Impuesto,'
                             'CuentaVtaExport,Etiqueta,CostoStdVino,Comentarios,Temp,NoVigente,Cvalle,'
                             'SinArte, CodigoExterno, Temp1,NombreFact,UnidadNegocio,Critico,Nac,TiempoProd,'
                             'Tolerancia,CodigoArancel,FechaEstado,KIT,EAN,DUM,Grado,AlturaEt, Capacidad,'
                             'NroBoletin, UltUsuarioMod,Detalle1,Detalle2 from submarca order by CodProducto asc')
        print("(1) tabla submarca")
        registrosorigen = kupay_cursor.rowcount
        i = 0
        for CodProducto, Producto, CodMarca, CodVar, Cali, CostoSTDProd, Existencia, Id_PT, Moneda, Tipo_vino, Cosecha, \
            CtaCtble, CodLinea, CCosto, Impuesto, CuentaVtaExport, Etiqueta, CostoStdVino, Comentarios, Temp, NoVigente, \
            Cvalle, SinArte, CodigoExterno, Temp1, NombreFact, UnidadNegocio, Critico, Nac, TiempoProd, Tolerancia, \
            CodigoArancel, FechaEstado, KIT, EAN, DUM, Grado, AlturaEt, Capacidad, NroBoletin, UltUsuarioMod, \
            Detalle1, Detalle2 in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_submarca (CodProducto, Producto,CodMarca,CodVar,Cali, " \
                                               "CostoSTDProd,Existencia,Id_PT, Moneda,Tipo_vino,Cosecha,CtaCtble, " \
                                               "CodLinea,CCosto,Impuesto,CuentaVtaExport,Etiqueta,CostoStdVino," \
                                               "Comentarios,Temp,NoVigente,Cvalle,SinArte, CodigoExterno, Temp1," \
                                               "NombreFact,UnidadNegocio, Critico,Nac,TiempoProd,Tolerancia," \
                                               "CodigoArancel, FechaEstado,KIT,EAN,DUM,Grado," \
                                               "AlturaEt, Capacidad,NroBoletin, UltUsuarioMod,Detalle1,Detalle2) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodProducto, Producto, CodMarca, CodVar, Cali, CostoSTDProd, Existencia, Id_PT, Moneda, Tipo_vino,
                   Cosecha, CtaCtble, CodLinea, CCosto, Impuesto, CuentaVtaExport, Etiqueta, CostoStdVino, Comentarios,
                   Temp, NoVigente, Cvalle, SinArte, CodigoExterno, Temp1, NombreFact, UnidadNegocio, Critico, Nac,
                   TiempoProd, Tolerancia, CodigoArancel, FechaEstado, KIT, EAN, DUM, Grado, AlturaEt, Capacidad,
                   NroBoletin, UltUsuarioMod, Detalle1, Detalle2)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla submarca: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                           " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'submarca', 'bdg_submarca', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA EMPRESAS 2
        i = 0
        kupay_cursor.execute(
            'select CodigoEmp, RUT, RazonSocial, Direccion, CorrBoletas, CorrGuias, CorrFacturas, CorrNCredNac, '
            'CorrInstructivo, CorrProforma, CorrFactExp, Pais, Fono, Fax, eMail, SitioWeb, Ciudad, RepresentanteLegal, '
            'Giro, CorrNCredExp, EmpRol, CorrNDebNac, CorrNDebExp, CodigoFin700, MembreteProforma, Comuna, CorrFactExenta, '
            'CorrGuiasPre from empresas')
        print("(2) tabla empresas")
        registrosorigen = kupay_cursor.rowcount
        for CodigoEmp, RUT, RazonSocial, Direccion, CorrBoletas, CorrGuias, CorrFacturas, CorrNCredNac, CorrInstructivo, \
            CorrProforma, CorrFactExp, Pais, Fono, Fax, eMail, SitioWeb, Ciudad, RepresentanteLegal, Giro, CorrNCredExp, \
            EmpRol, CorrNDebNac, CorrNDebExp, CodigoFin700, MembreteProforma, Comuna, CorrFactExenta, \
            CorrGuiasPre in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_empresas (CodigoEmp, RUT, RazonSocial, Direccion, " \
                                               " CorrBoletas, CorrGuias, CorrFacturas, CorrNCredNac, CorrInstructivo, " \
                                               "CorrProforma, CorrFactExp, Pais, Fono, Fax, eMail, SitioWeb, Ciudad, " \
                                               " RepresentanteLegal, Giro, CorrNCredExp, EmpRol, CorrNDebNac, " \
                                               " CorrNDebExp, CodigoFin700, MembreteProforma, Comuna, " \
                                               " CorrFactExenta, CorrGuiasPre) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s)"
            val = (CodigoEmp, RUT, RazonSocial, Direccion, CorrBoletas, CorrGuias, CorrFacturas, CorrNCredNac,
                   CorrInstructivo, CorrProforma, CorrFactExp, Pais, Fono, Fax, eMail, SitioWeb, Ciudad,
                   RepresentanteLegal, Giro, CorrNCredExp, EmpRol, CorrNDebNac, CorrNDebExp, CodigoFin700,
                   MembreteProforma, Comuna, CorrFactExenta, CorrGuiasPre)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla empresas: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES" \
                                           " (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'empresas', 'bdg_empresas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA MONEDAS 3
        i = 0
        kupay_cursor.execute(
            'select CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, SOLIdentificador, SOLTipoTabla '
            'from monedas')
        print("(3) tabla monedas")
        registrosorigen = kupay_cursor.rowcount
        for CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, SOLIdentificador, \
            SOLTipoTabla in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_monedas (CodMon, TC, Moneda, Decimales, ABREV, TCPresup, " \
                                               "DescribeIngles, SOLIdentificador, SOLTipoTabla) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodMon, TC, Moneda, Decimales, ABREV, TCPresup, DescribeIngles, SOLIdentificador, SOLTipoTabla)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla monedas: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'monedas', 'bdg_monedas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA VENDEDOR 4
        i = 0
        kupay_cursor.execute('select CodVend, Nombre, Fono, Mail, Canal, Ccosto, CodBodAs, Web, RutVend from vendedor')
        print("(4) tabla vendedor")
        registrosorigen = kupay_cursor.rowcount
        for CodVend, Nombre, Fono, Mail, Canal, Ccosto, CodBodAs, Web, RutVend in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_vendedor (CodVend, Nombre, Fono, Mail, Canal, " \
                                               "Ccosto, CodBodAs, Web, RutVend) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodVend, Nombre, Fono, Mail, Canal, Ccosto, CodBodAs, Web, RutVend)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla vendedor: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                           " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'vendedor', 'bdg_vendedor', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA MARCAS 5
        i = 0
        kupay_cursor.execute('select CodMarca, NomMarc, NoVigente, FechaEstado, UltUsuarioMod from marcas')
        registrosorigen = kupay_cursor.rowcount
        print("(5) tabla marcas")
        for CodMarca, NomMarc, NoVigente, FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_marcas (CodMarca, NomMarc, NoVigente, " \
                                               "FechaEstado, UltUsuarioMod) " \
                                               "VALUES (%s, %s, %s, %s, %s)"
            val = (CodMarca, NomMarc, NoVigente, FechaEstado, UltUsuarioMod)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla marcas: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                           " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'marcas', 'bdg_marcas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA CALIDAD 6
        i = 0
        kupay_cursor.execute(
            'select CodCal, NomCal, Codigo2, Orden, NoVigente, FechaEstado, UltUsuarioMod, TipoCal from calidad')
        registrosorigen = kupay_cursor.rowcount
        print("(6) tabla calidad")
        for CodCal, NomCal, Codigo2, Orden, NoVigente, FechaEstado, UltUsuarioMod, TipoCal in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_calidad (CodCal, NomCal, Codigo2, Orden, NoVigente, FechaEstado," \
                                               " UltUsuarioMod, TipoCal) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodCal, NomCal, Codigo2, Orden, NoVigente, FechaEstado, UltUsuarioMod, TipoCal)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla calidad: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'calidad', 'bdg_calidad', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA LINEAS 7
        i = 0
        kupay_cursor.execute(
            'select Codigo, Linea, CuentaPp, NoVigente, CuentaMuestras, CuentaTestigos, CtaMermas, Botella, Corcho, Grupo, '
            'Temp, Clase, UnidadNegocio, Orden, CCosto, FechaEstado, UltUsuarioMod from lineas')
        registrosorigen = kupay_cursor.rowcount
        print("(7) tabla lineas")
        for Codigo, Linea, CuentaPp, NoVigente, CuentaMuestras, CuentaTestigos, CtaMermas, Botella, Corcho, Grupo, Temp, \
            Clase, UnidadNegocio, Orden, CCosto, FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_lineas (Codigo, Linea, CuentaPp, NoVigente, CuentaMuestras, " \
                                               "CuentaTestigos, CtaMermas, Botella, Corcho, Grupo, Temp, Clase, " \
                                               "UnidadNegocio, Orden, CCosto, FechaEstado, UltUsuarioMod) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Codigo, Linea, CuentaPp, NoVigente, CuentaMuestras, CuentaTestigos, CtaMermas, Botella, Corcho,
                   Grupo, Temp, Clase, UnidadNegocio, Orden, CCosto, FechaEstado, UltUsuarioMod)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla lineas: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'lineas', 'bdg_lineas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA VARIEDAD 8
        i = 0
        kupay_cursor.execute(
            'select CodVar, NomVariedad, FactLts, FactGota, FactPren, TabBrix, TipoTabla, Tipo, RendFinal, FgotaMaceracion,'
            ' FprensaMaceracion, LitrosVendimia, KilosVendimia, Cuenta, varCodExterno, NoVigente, FechaEstado, '
            'UltUsuarioMod, CodFamilia, DescribeValle, CodCal, CodRV, Cod_Reg, Cod_Zona, Cod_Area from variedad')
        registrosorigen = kupay_cursor.rowcount
        print("(8) tabla variedad")
        for CodVar, NomVariedad, FactLts, FactGota, FactPren, TabBrix, TipoTabla, Tipo, RendFinal, FgotaMaceracion, \
            FprensaMaceracion, LitrosVendimia, KilosVendimia, Cuenta, varCodExterno, NoVigente, FechaEstado, \
            UltUsuarioMod, CodFamilia, DescribeValle, CodCal, CodRV, Cod_Reg, Cod_Zona, \
            Cod_Area in kupay_cursor.fetchall():
            i = i + 1
            if (math.isinf(LitrosVendimia))  :
                LitrosVendimia = 0

            sql = "INSERT INTO " + EsquemaBD + ".bdg_variedad (CodVar, NomVariedad, FactLts, FactGota, FactPren, " \
                                               "TabBrix, TipoTabla, Tipo, RendFinal, FgotaMaceracion, FprensaMaceracion," \
                                               " LitrosVendimia, KilosVendimia, Cuenta, varCodExterno, NoVigente, " \
                                               "FechaEstado, UltUsuarioMod, CodFamilia, DescribeValle, CodCal, CodRV, " \
                                               "Cod_Reg, Cod_Zona, Cod_Area) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodVar, NomVariedad, FactLts, FactGota, FactPren, TabBrix, TipoTabla, Tipo, RendFinal, FgotaMaceracion,
                   FprensaMaceracion, LitrosVendimia, KilosVendimia, Cuenta, varCodExterno, NoVigente, FechaEstado,
                   UltUsuarioMod, CodFamilia, DescribeValle, CodCal, CodRV, Cod_Reg, Cod_Zona, Cod_Area)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla variedad: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                           " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'variedad', 'bdg_variedad', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA VINOSYMOSTOS 9
        i = 0
        kupay_cursor.execute(
            'select TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp, Madera,'
            ' NoVigente, FechaEstado, UltUsuarioMod from vinosymostos')
        registrosorigen = kupay_cursor.rowcount
        print("(9) tabla vinosymostos")
        for TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp, Madera, NoVigente, \
            FechaEstado, UltUsuarioMod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_vinosymostos (TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP," \
                                               " CCosto, CodVar, CaidaDens, DevTemp, Madera, NoVigente, " \
                                               "FechaEstado, UltUsuarioMod) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (TipoVino, NombreVino, Tipo, PerDegus, Litros, CostoP, CCosto, CodVar, CaidaDens, DevTemp,
                   Madera, NoVigente, FechaEstado, UltUsuarioMod)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla vinosymostos: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino," \
                                           " NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'vinosymostos', 'bdg_vinosymostos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA estadosVenta 10
        i = 0
        kupay_cursor.execute('select evEstado, evDescripcion from estadosVenta')
        registrosorigen = kupay_cursor.rowcount
        print("(10) tabla estadosVenta")
        for evEstado, evDescripcion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_estadosVenta(evEstado, evDescripcion) VALUES (%s, %s)"
            val = (evEstado, evDescripcion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla estadosVenta: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'estadosVenta', 'bdg_estadosVenta', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA ClienteNac 11
        i = 0
        kupay_cursor.execute(
            'SELECT CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro, Region, Telefono,'
            ' Fax, Comuna, TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario, Casilla, PrecioObligado,'
            ' TotalVta, TotalCjs,  CodMon, Canal, Cod_Giro, Cod_Comuna, Cod_Ciudad, Cod_Pais, Cod_CargoContacto, FPago,'
            ' NoVigente, Bloqueado, LimiteCredito,  FechaEstado, UltUsuarioMod, MontoLCreditoRef, AnticipoReq,'
            ' PorcAnticipo, DiasMora FROM clientenac')
        registrosorigen = kupay_cursor.rowcount
        print("(11) tabla clientenac")
        for CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro, Region, Telefono, Fax, Comuna, \
            TipoCliente, Contacto, CargoContacto, FonoContacto, eMail, Comentario, Casilla, PrecioObligado, TotalVta, \
            TotalCjs, CodMon, Canal, Cod_Giro, Cod_Comuna, Cod_Ciudad, Cod_Pais, Cod_CargoContacto, FPago, NoVigente, \
            Bloqueado, LimiteCredito, FechaEstado, UltUsuarioMod, MontoLCreditoRef, AnticipoReq, PorcAnticipo, \
            DiasMora in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_clientenac (CodClienteNac, Nombre, Fantasia, Direccion, Mercado," \
                                               " CodVendedor, Ciudad, Giro, Region, Telefono, Fax, Comuna, TipoCliente," \
                                               " Contacto, CargoContacto, FonoContacto, eMail, Comentario, Casilla, " \
                                               "PrecioObligado, TotalVta, TotalCjs,  CodMon, Canal, Cod_Giro, Cod_Comuna," \
                                               " Cod_Ciudad, Cod_Pais, Cod_CargoContacto, FPago, NoVigente, Bloqueado," \
                                               " LimiteCredito,  FechaEstado, UltUsuarioMod, MontoLCreditoRef," \
                                               " AnticipoReq, PorcAnticipo, DiasMora) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s)"
            val = (CodClienteNac, Nombre, Fantasia, Direccion, Mercado, CodVendedor, Ciudad, Giro,
                   Region, Telefono, Fax, Comuna, TipoCliente, Contacto, CargoContacto, FonoContacto,
                   eMail, Comentario, Casilla, PrecioObligado, TotalVta, TotalCjs, CodMon, Canal,
                   Cod_Giro, Cod_Comuna, Cod_Ciudad, Cod_Pais, Cod_CargoContacto, FPago, NoVigente,
                   Bloqueado, LimiteCredito, FechaEstado, UltUsuarioMod, MontoLCreditoRef,
                   AnticipoReq, PorcAnticipo, DiasMora)
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

        # TABLA Clientes 12
        i = 0
        kupay_cursor.execute(
            'SELECT CodCliente,Nombre,Fantasia,Direccion,Mercado,Idioma,CodVendedor,Estado,Ciudad,Giro,Telefono,Fax,'
            'Agente,Contacto,Incoterm,FormaPagoNom,Pais,ContactoAgente,Recibidor,eMail,Comentario,CodPostal,'
            'PrecioCliente,TotalVta,LimiteCredito,DominioEmail,CodMon,TotalCjs,MarcaCaja,Cod_Giro,NoVigente,Cod_Ciudad,'
            'FechaEstado,CCosto,Bloqueado,OtroPaisVSC,RutContabilizacion,CodigoBaaNVSC,UltUsuarioMod,CodigoPais,'
            'ModVentaSII,MontoLCreditoRef,AnticipoReq,PorcAnticipo,CodClausulaVta, FormaPagoCod, DiasMora FROM clientes')
        registrosorigen = kupay_cursor.rowcount
        print("(12) tabla clientes")
        for CodCliente, Nombre, Fantasia, Direccion, Mercado, Idioma, CodVendedor, Estado, Ciudad, Giro, Telefono, Fax, \
            Agente, Contacto, Incoterm, FormaPagoNom, Pais, ContactoAgente, Recibidor, eMail, Comentario, CodPostal, \
            PrecioCliente, TotalVta, LimiteCredito, DominioEmail, CodMon, TotalCjs, MarcaCaja, Cod_Giro, NoVigente, \
            Cod_Ciudad, FechaEstado, CCosto, Bloqueado, OtroPaisVSC, RutContabilizacion, CodigoBaaNVSC, UltUsuarioMod, \
            CodigoPais, ModVentaSII, MontoLCreditoRef, AnticipoReq, PorcAnticipo, CodClausulaVta, FormaPagoCod, \
            DiasMora in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_clientes(CodCliente,Nombre,Fantasia,Direccion,Mercado,Idioma," \
                                               "CodVendedor,Estado,Ciudad,Giro,Telefono,Fax,Agente,Contacto,Incoterm," \
                                               "FormaPagoNom,Pais,ContactoAgente,Recibidor,eMail,Comentario,CodPostal," \
                                               "PrecioCliente,TotalVta,LimiteCredito,DominioEmail,CodMon,TotalCjs," \
                                               "MarcaCaja,Cod_Giro,NoVigente,Cod_Ciudad,FechaEstado,CCosto,Bloqueado," \
                                               "OtroPaisVSC,RutContabilizacion,CodigoBaaNVSC,UltUsuarioMod,CodigoPais," \
                                               "ModVentaSII,MontoLCreditoRef,AnticipoReq,PorcAnticipo,CodClausulaVta," \
                                               " FormaPagoCod, DiasMora) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodCliente, Nombre, Fantasia, Direccion, Mercado, Idioma, CodVendedor, Estado, Ciudad,
                   Giro, Telefono, Fax, Agente, Contacto, Incoterm, FormaPagoNom, Pais, ContactoAgente,
                   Recibidor, eMail, Comentario, CodPostal, PrecioCliente, TotalVta, LimiteCredito,
                   DominioEmail, CodMon, TotalCjs, MarcaCaja, Cod_Giro, NoVigente, Cod_Ciudad, FechaEstado,
                   CCosto, Bloqueado, OtroPaisVSC, RutContabilizacion, CodigoBaaNVSC, UltUsuarioMod,
                   CodigoPais, ModVentaSII, MontoLCreditoRef, AnticipoReq, PorcAnticipo, CodClausulaVta,
                   FormaPagoCod, DiasMora)
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

        # TABLA Ficha_nac 13
        i = 0
        kupay_cursor.execute(
            'SELECT NumFichaNac,FechaIngre,CodClienteNac,FechaEntrega,Neto,IVA,ILA,Total,evEstado,QtyVen,QtyMu,QtyTot,'
            'AutorizadoPor,FormaPagoCod,CodVend,LugarEntrega,FechaHora,Comentario,FechaODC,TotNetoMu,TotNetoEmb,'
            'TotNetoVen,QtyEmb,NomCliNac,DiasVenc,ValorConIVA,Despachado,CodEnc,CodMon,ClieBloqueado,SinPedido,'
            'ContratoVta,OrdenCompra,CodigoEmp,CodSucursal,TasaRef,Etique,ConfBodega,TotBotellas,PromVen,TotFactura,'
            'TotMueDcto,CodBod,Ruta,FechaDespacho,SoloInventario,IndicadorMora,SaldoCredito,ETDReqNac,FechaAutoriza '
            'FROM Ficha_nac')
        registrosorigen = kupay_cursor.rowcount
        print("(13) tabla Ficha_nac")
        for NumFichaNac, FechaIngre, CodClienteNac, FechaEntrega, Neto, IVA, ILA, Total, evEstado, QtyVen, QtyMu, QtyTot, \
            AutorizadoPor, FormaPagoCod, CodVend, LugarEntrega, FechaHora, Comentario, FechaODC, TotNetoMu, TotNetoEmb, \
            TotNetoVen, QtyEmb, NomCliNac, DiasVenc, ValorConIVA, Despachado, CodEnc, CodMon, ClieBloqueado, SinPedido, \
            ContratoVta, OrdenCompra, CodigoEmp, CodSucursal, TasaRef, Etique, ConfBodega, TotBotellas, PromVen, \
            TotFactura, TotMueDcto, CodBod, Ruta, FechaDespacho, SoloInventario, IndicadorMora, SaldoCredito, \
            ETDReqNac, FechaAutoriza in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_Ficha_nac (NumFichaNac,FechaIngre,CodClienteNac,FechaEntrega,Neto," \
                                               "IVA,ILA,Total,evEstado,QtyVen,QtyMu,QtyTot,AutorizadoPor,FormaPagoCod," \
                                               "CodVend,LugarEntrega,FechaHora,Comentario,FechaODC,TotNetoMu,TotNetoEmb," \
                                               "TotNetoVen,QtyEmb,NomCliNac,DiasVenc,ValorConIVA,Despachado,CodEnc," \
                                               "CodMon,ClieBloqueado,SinPedido,ContratoVta,OrdenCompra,CodigoEmp," \
                                               "CodSucursal,TasaRef,Etique,ConfBodega,TotBotellas,PromVen,TotFactura," \
                                               "TotMueDcto,CodBod,Ruta,FechaDespacho,SoloInventario,IndicadorMora," \
                                               "SaldoCredito,ETDReqNac,FechaAutoriza) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumFichaNac, FechaIngre, CodClienteNac, FechaEntrega, Neto, IVA, ILA, Total, evEstado, QtyVen,
                   QtyMu, QtyTot, AutorizadoPor, FormaPagoCod, CodVend, LugarEntrega, FechaHora, Comentario, FechaODC,
                   TotNetoMu, TotNetoEmb, TotNetoVen, QtyEmb, NomCliNac, DiasVenc, ValorConIVA, Despachado, CodEnc,
                   CodMon, ClieBloqueado, SinPedido, ContratoVta, OrdenCompra, CodigoEmp, CodSucursal, TasaRef, Etique,
                   ConfBodega, TotBotellas, PromVen, TotFactura, TotMueDcto, CodBod, Ruta, FechaDespacho, SoloInventario,
                   IndicadorMora, SaldoCredito, ETDReqNac, FechaAutoriza)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla Ficha_nac: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, " \
                                           "%s, %s, %s) "
        val = (Identificador, SistemaOrigen, 'Ficha_nac', 'bdg_Ficha_nac', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA Ficha_exp 14
        i = 0
        kupay_cursor.execute(
            'SELECT NumFicha,CodCliente,FechaP_O,P_O,Mercado,Incoterm,Vencimiento,FormaPagoNom,Pallets,Mater_Pos,'
            'Comentario,ETDNave,ETACliente,ETDVigna,FechaSTK,Nave,CodMon,Agente,FechaODC,evEstado,Etique,QtyVen,QtyMu,'
            'QtyTot,TotVenCom,TotMueDcto,TotVenBru,TotMueBru,NomCliExp,PromVen,Embarcador,Out,CodVend,Cod_Banco,'
            'SinPedido,ContratoVta,ConfBodega,PtoDestino,Web,CodEnc,FechaIngre,CodigoEmp,ClausulaVta,Consignar,Notify,'
            'TasaRef, CodConsigna,MarcaCaja,CodSucursal,TotFacturaMnd,TotFactura,TotBotellas,ETDReq,Seguro,Flete,'
            'ETDTransporte,PtoEmbarque,Cajas9Lts,CodAgenteAduana,Booking,DestinoCarga,DespachoExterno,CodBod,CodAdonix,'
            'BL_CRT,VI1,ORIGEN,BOLETIN,PACKING_LIST,CERTIF_PESO,EUR1,MANIF_CARGA,LIBRE_VENTA,AutorizadoPor,ClieBloqueado,'
            'IndicadorMora,SaldoCredito,FormaPagoCod,OtrosGastos,ArancelesImpuestos,HoraInicioSTK,FechaCorteDoc,'
            'HoraCorteDoc,FechaTerminoSTK,HoraTerminoSTK,NomDepContenedor,FechaAutoriza FROM Ficha_exp')
        registrosorigen = kupay_cursor.rowcount
        print("(14) tabla Ficha_exp")
        for NumFicha, CodCliente, FechaP_O, P_O, Mercado, Incoterm, Vencimiento, FormaPagoNom, Pallets, Mater_Pos, \
            Comentario, ETDNave, ETACliente, ETDVigna, FechaSTK, Nave, CodMon, Agente, FechaODC, evEstado, Etique, QtyVen, \
            QtyMu, QtyTot, TotVenCom, TotMueDcto, TotVenBru, TotMueBru, NomCliExp, PromVen, Embarcador, Out, CodVend, \
            Cod_Banco, SinPedido, ContratoVta, ConfBodega, PtoDestino, Web, CodEnc, FechaIngre, CodigoEmp, ClausulaVta, \
            Consignar, Notify, TasaRef, CodConsigna, MarcaCaja, CodSucursal, TotFacturaMnd, TotFactura, TotBotellas, \
            ETDReq, Seguro, Flete, ETDTransporte, PtoEmbarque, Cajas9Lts, CodAgenteAduana, Booking, DestinoCarga, \
            DespachoExterno, CodBod, CodAdonix, BL_CRT, VI1, ORIGEN, BOLETIN, PACKING_LIST, CERTIF_PESO, EUR1, \
            MANIF_CARGA, LIBRE_VENTA, AutorizadoPor, ClieBloqueado, IndicadorMora, SaldoCredito, FormaPagoCod, \
            OtrosGastos, ArancelesImpuestos, HoraInicioSTK, FechaCorteDoc, HoraCorteDoc, FechaTerminoSTK, \
            HoraTerminoSTK, NomDepContenedor, FechaAutoriza in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_Ficha_exp(NumFicha,CodCliente,FechaP_O,P_O,Mercado,Incoterm," \
                                               "Vencimiento,FormaPagoNom,Pallets,Mater_Pos,Comentario,ETDNave,ETACliente," \
                                               "ETDVigna,FechaSTK,Nave,CodMon,Agente,FechaODC,evEstado,Etique,QtyVen," \
                                               "QtyMu,QtyTot,TotVenCom,TotMueDcto,TotVenBru,TotMueBru,NomCliExp,PromVen," \
                                               "Embarcador,Out_ficha,CodVend,Cod_Banco,SinPedido,ContratoVta,ConfBodega," \
                                               "PtoDestino,Web,CodEnc,FechaIngre,CodigoEmp,ClausulaVta,Consignar,Notify," \
                                               "TasaRef, CodConsigna,MarcaCaja,CodSucursal,TotFacturaMnd,TotFactura," \
                                               "TotBotellas,ETDReq,Seguro,Flete,ETDTransporte,PtoEmbarque,Cajas9Lts," \
                                               "CodAgenteAduana,Booking,DestinoCarga,DespachoExterno,CodBod,CodAdonix," \
                                               "BL_CRT,VI1,ORIGEN,BOLETIN,PACKING_LIST,CERTIF_PESO,EUR1,MANIF_CARGA," \
                                               "LIBRE_VENTA,AutorizadoPor,ClieBloqueado,IndicadorMora,SaldoCredito," \
                                               "FormaPagoCod,OtrosGastos,ArancelesImpuestos,HoraInicioSTK,FechaCorteDoc," \
                                               "HoraCorteDoc,FechaTerminoSTK,HoraTerminoSTK,NomDepContenedor," \
                                               "FechaAutoriza) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s) "
            val = (NumFicha, CodCliente, FechaP_O, P_O, Mercado, Incoterm, Vencimiento, FormaPagoNom,
                   Pallets, Mater_Pos, Comentario, ETDNave, ETACliente, ETDVigna, FechaSTK, Nave, CodMon,
                   Agente, FechaODC, evEstado, Etique, QtyVen, QtyMu, QtyTot, TotVenCom, TotMueDcto,
                   TotVenBru, TotMueBru, NomCliExp, PromVen, Embarcador, Out, CodVend, Cod_Banco, SinPedido,
                   ContratoVta, ConfBodega, PtoDestino, Web, CodEnc, FechaIngre, CodigoEmp, ClausulaVta,
                   Consignar, Notify, TasaRef, CodConsigna, MarcaCaja, CodSucursal, TotFacturaMnd, TotFactura,
                   TotBotellas, ETDReq, Seguro, Flete, ETDTransporte, PtoEmbarque, Cajas9Lts, CodAgenteAduana,
                   Booking, DestinoCarga, DespachoExterno, CodBod, CodAdonix, BL_CRT, VI1, ORIGEN, BOLETIN,
                   PACKING_LIST, CERTIF_PESO, EUR1, MANIF_CARGA, LIBRE_VENTA, AutorizadoPor, ClieBloqueado,
                   IndicadorMora, SaldoCredito, FormaPagoCod, OtrosGastos, ArancelesImpuestos, HoraInicioSTK,
                   FechaCorteDoc, HoraCorteDoc, FechaTerminoSTK, HoraTerminoSTK, NomDepContenedor, FechaAutoriza)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla Ficha_exp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, " \
                                           "%s, %s, %s) "
        val = (Identificador, SistemaOrigen, 'Ficha_exp', 'bdg_Ficha_exp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA INSEMBARQUE 15
        i = 0
        kupay_cursor.execute(
            'SELECT Numero,Orden,TotalFOB,LugarCarga,ETD,Para,PtoEmbar,PtoDestino,FormaPagoNom,Fecha_Ins,Hora_Ins,'
            'Descrip_Cli,NFicha,Notify,CodCliente,Certif,Stacking,CerOrig,Observacion,Naviera,Forwarders,PesoBrutoC,'
            'PesoNetoC,Booking,Container,Consignar,ModVenta,ClausulaCpra,FleteMaritimo,Empaque,Botella,TotalCajas,CodMon,'
            'Nave,Firma,CodigoEmp,CorrEmpresa,ContenedorNum,Deposito,Transporte,Presentarse,EmbarqueVia,Marcas,Flete,'
            'Seguro,ComisionExterior,FormaPagoFlete,NumCartaCredito,CorrMandato,EncabMandato,OtrosGastos,'
            'ArancelesImpuestos FROM INSEMBARQUE')
        registrosorigen = kupay_cursor.rowcount
        print("(15) tabla INSEMBARQUE")
        for Numero, Orden, TotalFOB, LugarCarga, ETD, Para, PtoEmbar, PtoDestino, FormaPagoNom, Fecha_Ins, Hora_Ins, \
            Descrip_Cli, NFicha, Notify, CodCliente, Certif, Stacking, CerOrig, Observacion, Naviera, Forwarders, \
            PesoBrutoC, PesoNetoC, Booking, Container, Consignar, ModVenta, ClausulaCpra, FleteMaritimo, Empaque, Botella, \
            TotalCajas, CodMon, Nave, Firma, CodigoEmp, CorrEmpresa, ContenedorNum, Deposito, Transporte, Presentarse, \
            EmbarqueVia, Marcas, Flete, Seguro, ComisionExterior, FormaPagoFlete, NumCartaCredito, CorrMandato, \
            EncabMandato, OtrosGastos, ArancelesImpuestos in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_INSEMBARQUE (Numero,Orden,TotalFOB,LugarCarga,ETD,Para,PtoEmbar," \
                                               "PtoDestino,FormaPagoNom,Fecha_Ins,Hora_Ins,Descrip_Cli,NFicha,Notify," \
                                               "CodCliente,Certif,Stacking,CerOrig,Observacion,Naviera,Forwarders," \
                                               "PesoBrutoC,PesoNetoC,Booking,Container,Consignar,ModVenta,ClausulaCpra," \
                                               "FleteMaritimo,Empaque,Botella,TotalCajas,CodMon,Nave,Firma,CodigoEmp," \
                                               "CorrEmpresa,ContenedorNum,Deposito,Transporte,Presentarse,EmbarqueVia," \
                                               "Marcas,Flete,Seguro,ComisionExterior,FormaPagoFlete,NumCartaCredito," \
                                               "CorrMandato,EncabMandato,OtrosGastos,ArancelesImpuestos) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s)"
            val = (Numero, Orden, TotalFOB, LugarCarga, ETD, Para, PtoEmbar, PtoDestino, FormaPagoNom, Fecha_Ins, Hora_Ins,
                   Descrip_Cli, NFicha, Notify, CodCliente, Certif, Stacking, CerOrig, Observacion, Naviera, Forwarders,
                   PesoBrutoC, PesoNetoC, Booking, Container, Consignar, ModVenta, ClausulaCpra, FleteMaritimo, Empaque,
                   Botella, TotalCajas, CodMon, Nave, Firma, CodigoEmp, CorrEmpresa, ContenedorNum, Deposito, Transporte,
                   Presentarse, EmbarqueVia, Marcas, Flete, Seguro, ComisionExterior, FormaPagoFlete, NumCartaCredito,
                   CorrMandato, EncabMandato, OtrosGastos, ArancelesImpuestos)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla INSEMBARQUE: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'INSEMBARQUE', 'bdg_INSEMBARQUE', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detallemu_fexp 16
        i = 0
        kupay_cursor.execute(
            'SELECT NumFicha,Cantidad,Unidad,Valor,Bruto,MonDesc,TotalDcto,Mercado,Fecha,CodMarca,CodVar,Cosecha,'
            'TipoVino,Mes,NomMarca,NomVar,CantBotellas,CodProducto,Saldo,PorcDesc,CodDesc,Descripcion,ProdEmb,'
            'Neto,EsDcto,NLinea,Modificado FROM detallemu_fexp')
        registrosorigen = kupay_cursor.rowcount
        print("(16) tabla detallemu_fexp")
        for NumFicha, Cantidad, Unidad, Valor, Bruto, MonDesc, TotalDcto, Mercado, Fecha, CodMarca, CodVar, Cosecha, \
            TipoVino, Mes, NomMarca, NomVar, CantBotellas, CodProducto, Saldo, PorcDesc, CodDesc, Descripcion, \
            ProdEmb, Neto, EsDcto, NLinea, Modificado in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detallemu_fexp (NumFicha,Cantidad,Unidad,Valor,Bruto," \
                                               "MonDesc,TotalDcto,Mercado,Fecha,CodMarca,CodVar,Cosecha," \
                                               "TipoVino,Mes,NomMarca,NomVar,CantBotellas,CodProducto,Saldo," \
                                               "PorcDesc,CodDesc,Descripcion,ProdEmb,Neto,EsDcto,NLinea," \
                                               "Modificado) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumFicha, Cantidad, Unidad, Valor, Bruto, MonDesc, TotalDcto, Mercado, Fecha,
                   CodMarca, CodVar, Cosecha, TipoVino, Mes, NomMarca, NomVar, CantBotellas,
                   CodProducto, Saldo, PorcDesc, CodDesc, Descripcion, ProdEmb, Neto, EsDcto,
                   NLinea, Modificado)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detallemu_fexp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detallemu_fexp', 'bdg_detallemu_fexp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detallemu_fnac 17
        i = 0
        kupay_cursor.execute(
            'SELECT NumFichaNac,Cantidad,Unidad,Valor,DescAdic,CodMarca,CodVar,Cosecha,TipoVino,CodCal,PorcDesc,'
            'Neto,NomMarca,NomVar,CantBotellas,CodProducto,Saldo,ProdEmb,Mes,CodDesc,EsDcto,TotalDcto,MonDesc,'
            'NLinea,Modificado FROM detallemu_fnac')
        registrosorigen = kupay_cursor.rowcount
        print("(17) tabla detallemu_fnac")
        for NumFichaNac, Cantidad, Unidad, Valor, DescAdic, CodMarca, CodVar, Cosecha, TipoVino, CodCal, PorcDesc, \
            Neto, NomMarca, NomVar, CantBotellas, CodProducto, Saldo, ProdEmb, Mes, CodDesc, EsDcto, TotalDcto, \
            MonDesc, NLinea, Modificado in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detallemu_fnac (NumFichaNac,Cantidad,Unidad,Valor,DescAdic," \
                                               "CodMarca,CodVar,Cosecha,TipoVino,CodCal,PorcDesc,Neto,NomMarca," \
                                               "NomVar,CantBotellas,CodProducto,Saldo,ProdEmb,Mes,CodDesc,EsDcto," \
                                               "TotalDcto,MonDesc,NLinea,Modificado) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumFichaNac, Cantidad, Unidad, Valor, DescAdic, CodMarca, CodVar, Cosecha, TipoVino, CodCal, PorcDesc,
                   Neto, NomMarca, NomVar, CantBotellas, CodProducto, Saldo, ProdEmb, Mes, CodDesc, EsDcto, TotalDcto,
                   MonDesc, NLinea, Modificado)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detallemu_fnac: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen,NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detallemu_fnac', 'bdg_detallemu_fnac', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detalle_fac 18
        i = 0
        kupay_cursor.execute(
            'SELECT Numero,DF_Cant,DF_Unid,DT_Val,DT_Tot,DF_Marc,DF_Var,DF_Cos,DF_Cal,CodProducto,DF_Vino,'
            'DF_NPed,DF_Desc,DF_Adic,DF_ValDesc,DF_DesAdic,DF_Descripcion,Cuba,TipoMov,CtaVentas,TipoImp,'
            'MontoImp,CodConcepto,CostoVta,CCosto,Resta,EsDcto,DF_Neto,EsMuestra,CalculaILA,TieneLotes,'
            'IdDetalleFN,CtaCostoVta,CImputacion FROM detalle_fac ')

        registrosorigen = kupay_cursor.rowcount
        print("(18) tabla detalle_fac")
        for Numero, DF_Cant, DF_Unid, DT_Val, DT_Tot, DF_Marc, DF_Var, DF_Cos, DF_Cal, CodProducto, DF_Vino, DF_NPed, \
            DF_Desc, DF_Adic, DF_ValDesc, DF_DesAdic, DF_Descripcion, Cuba, TipoMov, CtaVentas, TipoImp, MontoImp, \
            CodConcepto, CostoVta, CCosto, Resta, EsDcto, DF_Neto, EsMuestra, CalculaILA, TieneLotes, IdDetalleFN, \
            CtaCostoVta, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_fac (Numero,DF_Cant,DF_Unid,DT_Val,DT_Tot," \
                                               "DF_Marc,DF_Var,DF_Cos,DF_Cal,CodProducto,DF_Vino,DF_NPed," \
                                               "DF_Desc,DF_Adic,DF_ValDesc,DF_DesAdic,DF_Descripcion,Cuba," \
                                               "TipoMov,CtaVentas,TipoImp,MontoImp,CodConcepto,CostoVta," \
                                               "CCosto,Resta,EsDcto,DF_Neto,EsMuestra,CalculaILA,TieneLotes," \
                                               "IdDetalleFN,CtaCostoVta,CImputacion) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s)"
            val = (Numero, DF_Cant, DF_Unid, DT_Val, DT_Tot, DF_Marc, DF_Var, DF_Cos, DF_Cal,
                   CodProducto, DF_Vino, DF_NPed, DF_Desc, DF_Adic, DF_ValDesc, DF_DesAdic,
                   DF_Descripcion, Cuba, TipoMov, CtaVentas, TipoImp, MontoImp, CodConcepto,
                   CostoVta, CCosto, Resta, EsDcto, DF_Neto, EsMuestra, CalculaILA, TieneLotes,
                   IdDetalleFN, CtaCostoVta, CImputacion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detalle_fac: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalle_fac', 'bdg_detalle_fac', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA facturas 19
        i = 0
        kupay_cursor.execute(
            'SELECT Numero,Fecha,Cli_Fac,Vend_Fac,NumFichaNac,Neto,Iva,Ila,Total,Est,Abonos,Saldo,NomCli,'
            'Vence,Pedido,NetoVe,NetoMu,NetoEmb,ConIva,Guia,Exenta,Nota,TipoDoc,Contrato,RebajaStock,'
            'Descuento,CodBod,FormaPagoCod,GlosaFact,PorcDesc,TotalImpuestos,PorcIva,CodigoEmp,CorrEmpresa,'
            'FechaProbPago,DiasVenc,bSelect,Lugarentrega ,Temp,numperiodo,SucCodigo,NumFolio,Ruta,CodOper,'
            'OrdenCpraFicha,FechaModificacion,QuienModificacion,Impresa,Centralizada,CodigoOC,CentralizaCV,'
            'FechaContabiliza,MotivoAnulacion,CDITotal,CDIIVA,CodCC,WS_NumCabID,WS_FolioSII,AutorizadoPor,'
            'ClieBloqueado,IndicadorMora,SaldoCredito,FechaAutoriza,CabLlgId,CabOpeNumero,NulaPorNC FROM facturas')
        registrosorigen = kupay_cursor.rowcount
        print("(19) tabla facturas")
        for Numero, Fecha, Cli_Fac, Vend_Fac, NumFichaNac, Neto, Iva, Ila, Total, Est, Abonos, Saldo, NomCli, \
            Vence, Pedido, NetoVe, NetoMu, NetoEmb, ConIva, Guia, Exenta, Nota, TipoDoc, Contrato, RebajaStock, \
            Descuento, CodBod, FormaPagoCod, GlosaFact, PorcDesc, TotalImpuestos, PorcIva, CodigoEmp, \
            CorrEmpresa, FechaProbPago, DiasVenc, bSelect, Lugarentrega, Temp, numperiodo, SucCodigo, \
            NumFolio, Ruta, CodOper, OrdenCpraFicha, FechaModificacion, QuienModificacion, Impresa, Centralizada, \
            CodigoOC, CentralizaCV, FechaContabiliza, MotivoAnulacion, CDITotal, CDIIVA, CodCC, WS_NumCabID, \
            WS_FolioSII, AutorizadoPor, ClieBloqueado, IndicadorMora, SaldoCredito, FechaAutoriza, CabLlgId, \
            CabOpeNumero, NulaPorNC in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_facturas (Numero,Fecha,Cli_Fac,Vend_Fac,NumFichaNac,Neto," \
                                               "Iva,Ila,Total,Est,Abonos,Saldo,NomCli,Vence,Pedido,NetoVe,NetoMu," \
                                               "NetoEmb,ConIva,Guia,Exenta,Nota,TipoDoc,Contrato,RebajaStock," \
                                               "Descuento,CodBod,FormaPagoCod,GlosaFact,PorcDesc,TotalImpuestos," \
                                               "PorcIva,CodigoEmp,CorrEmpresa,FechaProbPago,DiasVenc,bSelect," \
                                               "Lugarentrega ,Temp,numperiodo,SucCodigo,NumFolio,Ruta,CodOper ," \
                                               "OrdenCpraFicha,FechaModificacion,QuienModificacion,Impresa," \
                                               "Centralizada,CodigoOC,CentralizaCV,FechaContabiliza,MotivoAnulacion," \
                                               "CDITotal,CDIIVA,CodCC,WS_NumCabID,WS_FolioSII,AutorizadoPor," \
                                               "ClieBloqueado,IndicadorMora,SaldoCredito,FechaAutoriza,CabLlgId," \
                                               "CabOpeNumero,NulaPorNC) VALUES (%s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Numero, Fecha, Cli_Fac, Vend_Fac, NumFichaNac, Neto, Iva, Ila, Total, Est,
                   Abonos, Saldo, NomCli, Vence, Pedido, NetoVe, NetoMu, NetoEmb, ConIva, Guia,
                   Exenta, Nota, TipoDoc, Contrato, RebajaStock, Descuento, CodBod, FormaPagoCod,
                   GlosaFact, PorcDesc, TotalImpuestos, PorcIva, CodigoEmp, CorrEmpresa, FechaProbPago,
                   DiasVenc, bSelect, Lugarentrega, Temp, numperiodo, SucCodigo, NumFolio, Ruta, CodOper,
                   OrdenCpraFicha, FechaModificacion, QuienModificacion, Impresa, Centralizada, CodigoOC,
                   CentralizaCV, FechaContabiliza, MotivoAnulacion, CDITotal, CDIIVA, CodCC, WS_NumCabID,
                   WS_FolioSII, AutorizadoPor, ClieBloqueado, IndicadorMora, SaldoCredito, FechaAutoriza,
                   CabLlgId, CabOpeNumero, NulaPorNC)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla facturas: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'facturas', 'bdg_facturas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA factura_expo 20
        i = 0
        kupay_cursor.execute(
            'SELECT NumeroFactExp,Fecha,CodCliente,Moneda,TotalMercaderia,FormaPagoNom,FechaVence,Transporte,'
            'Embarque,BillOfLanding,InfoExportacion,PesoNeto,PesoBruto,Volumen,NumCajas,Marcas,PtoDestino,'
            'NumeroFicha,Estado,Abonos,Saldo,NombreCliente,Observaciones,Nave,CondicionesVta,TotalClausulaVta,'
            'TotalSeguro,TotalFlete,TotalUSD,PaisDestino,NumOrden,UnidadMedNeto,UnidadMedBruto,OurBank,Agency,'
            'AccountNumber,SwiftCode,Address,AccountExecut,MotivoAnulacion,TasaUSD,TasaPesosUSD,Cod_Banco,'
            'NumeroProforma,CAST(totalpesos AS float) as TotalPesos,Tipo_Doc,PorcDesc1,GlosaDesc1,PorcDesc2,'
            'GlosaDesc2,ETDNave,NumConteiner,Sello,MontoDesc1,MontoDesc2,DestinoCarga,TotalGlosa,NumeroPedido,'
            'CodigoEmp,CorrEmpresa,GlosaClausulaVta,GlosaTotSeguro,GlosaTotFlete,GlosaTotUSD,ClausulaVta,TotalFOC,'
            'TotalaPago,Vendedor,Mercado,CodCliFicha,FE_ABBA,RebajaExist,numperiodo,CodOper,FechaModificacion,'
            'QuienModificacion,FechaTasaUSD,FechaTasaPesosUSD,Centralizada,CodigoOC,CentralizaCV,'
            'FechaContabiliza,CDITotal,CDISeguro,CDIFlete,CodCC,CodCondPago,WS_NumCabID,WS_FolioSII,'
            'CodModVenta,CodViaTransporte,FormaPagoCod,OtrosGastos,ArancelesImpuestos,CDIOtrGastos,'
            'CDIAImpuestos,CabLlpId,CabOpeNumero FROM factura_expo '
            'WHERE YEAR(ETDNave) <> 20065 order by NumeroFactExp asc')
        registrosorigen = kupay_cursor.rowcount
        print("(20) tabla factura_expo")
        for NumeroFactExp, Fecha, CodCliente, Moneda, TotalMercaderia, FormaPagoNom, FechaVence, Transporte, \
            Embarque, BillOfLanding, InfoExportacion, PesoNeto, PesoBruto, Volumen, NumCajas, Marcas, PtoDestino, \
            NumeroFicha, Estado, Abonos, Saldo, NombreCliente, Observaciones, Nave, CondicionesVta, \
            TotalClausulaVta, TotalSeguro, TotalFlete, TotalUSD, PaisDestino, NumOrden, UnidadMedNeto, \
            UnidadMedBruto, OurBank, Agency, AccountNumber, SwiftCode, Address, AccountExecut, MotivoAnulacion, \
            TasaUSD, TasaPesosUSD, Cod_Banco, NumeroProforma, TotalPesos, Tipo_Doc, PorcDesc1, GlosaDesc1, \
            PorcDesc2, GlosaDesc2, ETDNave, NumConteiner, Sello, MontoDesc1, MontoDesc2, DestinoCarga, \
            TotalGlosa, NumeroPedido, CodigoEmp, CorrEmpresa, GlosaClausulaVta, GlosaTotSeguro, GlosaTotFlete, \
            GlosaTotUSD, ClausulaVta, TotalFOC, TotalaPago, Vendedor, Mercado, CodCliFicha, FE_ABBA, \
            RebajaExist, numperiodo, CodOper, FechaModificacion, QuienModificacion, FechaTasaUSD, FechaTasaPesosUSD, \
            Centralizada, CodigoOC, CentralizaCV, FechaContabiliza, CDITotal, CDISeguro, CDIFlete, CodCC, \
            CodCondPago, WS_NumCabID, WS_FolioSII, CodModVenta, CodViaTransporte, FormaPagoCod, OtrosGastos, \
            ArancelesImpuestos, CDIOtrGastos, CDIAImpuestos, CabLlpId, CabOpeNumero in kupay_cursor.fetchall():
            i = i + 1
            if str(TotalPesos) == 'inf':
                TotalPesos = 0
            sql = "INSERT INTO " + EsquemaBD + ".bdg_factura_expo (NumeroFactExp,Fecha,CodCliente,Moneda," \
                                               "TotalMercaderia,FormaPagoNom,FechaVence,Transporte,Embarque," \
                                               "BillOfLanding,InfoExportacion,PesoNeto,PesoBruto,Volumen,NumCajas," \
                                               "Marcas,PtoDestino,NumeroFicha,Estado,Abonos,Saldo,NombreCliente," \
                                               "Observaciones,Nave,CondicionesVta,TotalClausulaVta,TotalSeguro," \
                                               "TotalFlete,TotalUSD,PaisDestino,NumOrden,UnidadMedNeto,UnidadMedBruto," \
                                               "OurBank,Agency,AccountNumber,SwiftCode,Address,AccountExecut," \
                                               "MotivoAnulacion,TasaUSD,TasaPesosUSD,Cod_Banco,NumeroProforma," \
                                               "TotalPesos,Tipo_Doc,PorcDesc1,GlosaDesc1,PorcDesc2,GlosaDesc2," \
                                               "ETDNave,NumConteiner,Sello,MontoDesc1,MontoDesc2,DestinoCarga," \
                                               "TotalGlosa,NumeroPedido,CodigoEmp,CorrEmpresa,GlosaClausulaVta," \
                                               "GlosaTotSeguro,GlosaTotFlete,GlosaTotUSD,ClausulaVta,TotalFOC," \
                                               "TotalaPago,Vendedor,Mercado,CodCliFicha,FE_ABBA,RebajaExist," \
                                               "numperiodo,CodOper,FechaModificacion,QuienModificacion," \
                                               "FechaTasaUSD,FechaTasaPesosUSD," \
                                               "Centralizada,CodigoOC,CentralizaCV,FechaContabiliza,CDITotal," \
                                               "CDISeguro,CDIFlete,CodCC,CodCondPago,WS_NumCabID,WS_FolioSII," \
                                               "CodModVenta,CodViaTransporte,FormaPagoCod,OtrosGastos,ArancelesImpuestos," \
                                               "CDIOtrGastos,CDIAImpuestos,CabLlpId,CabOpeNumero) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumeroFactExp, Fecha, CodCliente, Moneda, TotalMercaderia, FormaPagoNom, FechaVence, Transporte,
                   Embarque,
                   BillOfLanding, InfoExportacion, PesoNeto, PesoBruto, Volumen, NumCajas, Marcas, PtoDestino, NumeroFicha,
                   Estado, Abonos, Saldo, NombreCliente, Observaciones, Nave, CondicionesVta, TotalClausulaVta, TotalSeguro,
                   TotalFlete, TotalUSD, PaisDestino, NumOrden, UnidadMedNeto, UnidadMedBruto, OurBank, Agency,
                   AccountNumber,
                   SwiftCode, Address, AccountExecut, MotivoAnulacion, TasaUSD, TasaPesosUSD, Cod_Banco, NumeroProforma,
                   TotalPesos, Tipo_Doc, PorcDesc1, GlosaDesc1, PorcDesc2, GlosaDesc2, ETDNave, NumConteiner, Sello,
                   MontoDesc1, MontoDesc2, DestinoCarga, TotalGlosa, NumeroPedido, CodigoEmp, CorrEmpresa, GlosaClausulaVta,
                   GlosaTotSeguro, GlosaTotFlete, GlosaTotUSD, ClausulaVta, TotalFOC, TotalaPago, Vendedor,
                   Mercado, CodCliFicha, FE_ABBA, RebajaExist, numperiodo, CodOper, FechaModificacion, QuienModificacion,
                   FechaTasaUSD, FechaTasaPesosUSD, Centralizada, CodigoOC, CentralizaCV, FechaContabiliza, CDITotal,
                   CDISeguro, CDIFlete, CodCC, CodCondPago, WS_NumCabID, WS_FolioSII, CodModVenta, CodViaTransporte,
                   FormaPagoCod, OtrosGastos, ArancelesImpuestos,
                   CDIOtrGastos, CDIAImpuestos, CabLlpId, CabOpeNumero)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla factura_expo: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'factura_expo', 'bdg_factura_expo', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA deta_facexp 21
        i = 0
        kupay_cursor.execute(
            'SELECT NumeroFactExp,D_Cant,D_Descri,D_PrUnit,D_DTota,D_Unid,CodProducto,Desc1,DescCom,CodConcepto,'
            'CodigoExterno,Resta,CostoVta,EsDcto,NLinea,Ccosto,CtaVenta,EsMuestra,CodArancel,'
            'CAST(d_totalpesos as float) as D_TotalPesos,CtaCostoVta,CImputacion FROM deta_facexp ')

        registrosorigen = kupay_cursor.rowcount
        print("(21) tabla deta_facexp")
        for NumeroFactExp, D_Cant, D_Descri, D_PrUnit, D_DTota, D_Unid, CodProducto, Desc1, DescCom, CodConcepto, \
            CodigoExterno, Resta, CostoVta, EsDcto, NLinea, Ccosto, CtaVenta, EsMuestra, CodArancel, \
            D_TotalPesos, CtaCostoVta, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_deta_facexp (NumeroFactExp,D_Cant,D_Descri,D_PrUnit," \
                                               "D_DTota,D_Unid,CodProducto,Desc1,DescCom,CodConcepto,CodigoExterno," \
                                               "Resta,CostoVta,EsDcto,NLinea,Ccosto,CtaVenta,EsMuestra,CodArancel," \
                                               "D_TotalPesos,CtaCostoVta,CImputacion) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s,%s, %s)"
            val = (NumeroFactExp, D_Cant, D_Descri, D_PrUnit, D_DTota, D_Unid, CodProducto,
                   Desc1, DescCom, CodConcepto, CodigoExterno, Resta, CostoVta, EsDcto, NLinea,
                   Ccosto, CtaVenta, EsMuestra, CodArancel, D_TotalPesos, CtaCostoVta, CImputacion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla deta_facexp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'deta_facexp', 'bdg_deta_facexp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detalle_ficha 22
        i = 0
        kupay_cursor.execute(
            'SELECT NumFicha,Cantidad,Unidad,Valor,Bruto,PorcComision,MontoComision,Mercado,Fecha,CodMarca,CodVar,'
            'Cosecha,TipoVino,Mes,CantBotellas,CodProducto,Saldo,PorcDesc,CValle,CodCal,Neto,MonDesc,NLinea,Cjs9Lts,'
            'Grado,KIT,Modificado FROM detalle_ficha')
        registrosorigen = kupay_cursor.rowcount
        print("(22) tabla detalle_ficha")
        for NumFicha, Cantidad, Unidad, Valor, Bruto, PorcComision, MontoComision, Mercado, Fecha, CodMarca, \
            CodVar, Cosecha, TipoVino, Mes, CantBotellas, CodProducto, Saldo, PorcDesc, CValle, CodCal, Neto, \
            MonDesc, NLinea, Cjs9Lts, Grado, KIT, Modificado in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_ficha (NumFicha,Cantidad,Unidad,Valor,Bruto," \
                                               "PorcComision,MontoComision,Mercado,Fecha,CodMarca,CodVar,Cosecha," \
                                               "TipoVino,Mes,CantBotellas,CodProducto,Saldo,PorcDesc,CValle," \
                                               "CodCal,Neto,MonDesc,NLinea,Cjs9Lts,Grado,KIT,Modificado) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumFicha, Cantidad, Unidad, Valor, Bruto, PorcComision, MontoComision, Mercado,
                   Fecha, CodMarca, CodVar, Cosecha, TipoVino, Mes, CantBotellas, CodProducto,
                   Saldo, PorcDesc, CValle, CodCal, Neto, MonDesc, NLinea, Cjs9Lts, Grado, KIT, Modificado)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detalle_ficha: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalle_ficha', 'bdg_detalle_ficha', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detalle_fichanac 23
        i = 0
        kupay_cursor.execute(
            'SELECT NumFichaNac,Cantidad,Unidad,Valor,DescAdic,CodMarca,CodVar,Cosecha,TipoVino,CodCal,PorcDesc,Neto,'
            'CantBotellas,CodProducto,Saldo,Mes,MonDesc,NLinea,ValorOrig,Grado,KIT,Modificado FROM detalle_fichanac')
        registrosorigen = kupay_cursor.rowcount
        print("(23) tabla detalle_fichanac")
        for NumFichaNac, Cantidad, Unidad, Valor, DescAdic, CodMarca, CodVar, Cosecha, TipoVino, CodCal, PorcDesc, Neto, \
            CantBotellas, CodProducto, Saldo, Mes, MonDesc, NLinea, ValorOrig, \
            Grado, KIT, Modificado in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_fichanac (NumFichaNac,Cantidad,Unidad,Valor,DescAdic," \
                                               "CodMarca,CodVar,Cosecha,TipoVino,CodCal,PorcDesc,Neto,CantBotellas," \
                                               "CodProducto,Saldo,Mes,MonDesc,NLinea,ValorOrig,Grado,KIT,Modificado) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s)"
            val = (NumFichaNac, Cantidad, Unidad, Valor, DescAdic, CodMarca, CodVar, Cosecha, TipoVino, CodCal, PorcDesc,
                   Neto, CantBotellas, CodProducto, Saldo, Mes, MonDesc, NLinea, ValorOrig, Grado, KIT, Modificado)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detalle_fichanac: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura(id,SistemaOrigen,TablaOrigen,TablaDestino,NroRegistroOrigen," \
                                           " NroRegistroDestino, FechaCarga)" \
                                           " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detalle_fichanac', 'bdg_detalle_fichanac', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA deta_embfactn 24
        i = 0
        kupay_cursor.execute(
            'SELECT Det_EmbFN,FN_Cant,CodMat,FN_Valor,FN_Total,FNDesc1,FNDesc2,FN_Descrip,CodBod,CCosto,CtaVenta,CostoVta,'
            'FN_Neto,CtaCostoVta,CImputacion FROM deta_embfactn')
        registrosorigen = kupay_cursor.rowcount
        print("(24) tabla deta_embfactn")
        for Det_EmbFN, FN_Cant, CodMat, FN_Valor, FN_Total, FNDesc1, FNDesc2, FN_Descrip, CodBod, CCosto, CtaVenta, \
            CostoVta, FN_Neto, CtaCostoVta, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_deta_embfactn (Det_EmbFN,FN_Cant,CodMat,FN_Valor,FN_Total,FNDesc1," \
                                               "FNDesc2,FN_Descrip,CodBod,CCosto,CtaVenta,CostoVta,FN_Neto,CtaCostoVta," \
                                               "CImputacion) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Det_EmbFN, FN_Cant, CodMat, FN_Valor, FN_Total, FNDesc1, FNDesc2, FN_Descrip,
                   CodBod, CCosto, CtaVenta, CostoVta, FN_Neto, CtaCostoVta, CImputacion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla deta_embfactn: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura(id,SistemaOrigen,TablaOrigen,TablaDestino,NroRegistroOrigen," \
                                           "NroRegistroDestino, FechaCarga) VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'deta_embfactn', 'bdg_deta_embfactn', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA descuentosvta 25
        i = 0
        kupay_cursor.execute(
            'SELECT CodDesc,Concepto,PorcDef,MontoDef,TotFicha,Facturable,Tipo,Abrev,Producto,'
            'SobreFOBPdto,Orden,CtaCtbleVSC FROM descuentosvta')
        registrosorigen = kupay_cursor.rowcount
        print("(25) tabla descuentosvta")
        for CodDesc, Concepto, PorcDef, MontoDef, TotFicha, Facturable, Tipo, Abrev, Producto, \
            SobreFOBPdto, Orden, CtaCtbleVSC in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_descuentosvta (CodDesc,Concepto,PorcDef,MontoDef,TotFicha," \
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

        # TABLA materiales 26
        i = 0
        kupay_cursor.execute(
            'SELECT CodMat,Descripcion,Existencia,Precio,Fam,CapacBot,StocIn,TotalValor,Unidad,Minimo,Codigo_Ext,'
            'CodCtaCtble,CorrecMonetaria,Grupo,Fisico,CantidadInicial,CantidadFinal,TotMontoFinal,ErrorKardex,'
            'TomaInv,CodAuxiliar,ValorAuxiliar,EsSouvenir,MargenMerma,Codprov,CodCtaMerma,Cosecha,Grado,'
            'PathNorma,CodigoOrigen,CCostoMerma,NoVigente,FechaEstado,UltUsuarioMod,CodCtaGastos,'
            'tmpValorPMP FROM materiales')
        registrosorigen = kupay_cursor.rowcount
        print("(26) tabla materiales")
        for CodMat, Descripcion, Existencia, Precio, Fam, CapacBot, StocIn, TotalValor, Unidad, Minimo, Codigo_Ext, \
            CodCtaCtble, CorrecMonetaria, Grupo, Fisico, CantidadInicial, CantidadFinal, TotMontoFinal, \
            ErrorKardex, TomaInv, CodAuxiliar, ValorAuxiliar, EsSouvenir, MargenMerma, Codprov, CodCtaMerma, \
            Cosecha, Grado, PathNorma, CodigoOrigen, CCostoMerma, NoVigente, FechaEstado, UltUsuarioMod, \
            CodCtaGastos, tmpValorPMP in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_materiales (CodMat,Descripcion,Existencia,Precio,Fam,CapacBot," \
                                               "StocIn,TotalValor,Unidad,Minimo,Codigo_Ext,CodCtaCtble," \
                                               "CorrecMonetaria,Grupo,Fisico,CantidadInicial,CantidadFinal," \
                                               "TotMontoFinal,ErrorKardex,TomaInv,CodAuxiliar,ValorAuxiliar," \
                                               "EsSouvenir,MargenMerma,Codprov,CodCtaMerma,Cosecha,Grado," \
                                               "PathNorma,CodigoOrigen,CCostoMerma,NoVigente,FechaEstado," \
                                               "UltUsuarioMod,CodCtaGastos,tmpValorPMP) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s)"
            val = (CodMat, Descripcion, Existencia, Precio, Fam, CapacBot, StocIn, TotalValor,
                   Unidad, Minimo, Codigo_Ext, CodCtaCtble, CorrecMonetaria, Grupo, Fisico, CantidadInicial,
                   CantidadFinal, TotMontoFinal, ErrorKardex, TomaInv, CodAuxiliar, ValorAuxiliar,
                   EsSouvenir, MargenMerma, Codprov, CodCtaMerma, Cosecha, Grado, PathNorma, CodigoOrigen,
                   CCostoMerma, NoVigente, FechaEstado, UltUsuarioMod, CodCtaGastos, tmpValorPMP)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla materiales: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'materiales', 'bdg_materiales', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA Notacredito 27
        i = 0
        kupay_cursor.execute(
            'Select Numero, Fecha, CodCli, Factura, Neto, Iva, Total, IngStock, Ila, Glosa_NC, Tipo_Doc, Boleta, '
            'CodigoEmp, CorrEmpresa, PorcIVA, PorcILA, NumFolio, TipoDebito, Observaciones, Estado, CodOper, '
            'Centralizada, CodigoOC, numperiodo, CentralizaCV, FechaContabiliza, MotivoAnulacion, CodRefDTE, '
            'CDITotal, CDIIVA, CDIILA, Abonos, Saldo, bSelect, Temp, CodCC, WS_NumCabID, WS_FolioSII, CabLlgId, '
            'CabOpeNumero, NumFichaFac, NoReversaCV, NumInternoRef from notacredito')
        registrosorigen = kupay_cursor.rowcount
        print("(27) tabla Notacredito")
        for Numero, Fecha, CodCli, Factura, Neto, Iva, Total, IngStock, Ila, Glosa_NC, Tipo_Doc, Boleta, CodigoEmp, \
            CorrEmpresa, PorcIVA, PorcILA, NumFolio, TipoDebito, Observaciones, Estado, CodOper, Centralizada, \
            CodigoOC, num_periodo, CentralizaCV, FechaContabiliza, MotivoAnulacion, CodRefDTE, CDITotal, CDIIVA, \
            CDIILA, Abonos, Saldo, bSelect, Temp, CodCC, WS_NumCabID, WS_FolioSII, CabLlgId, CabOpeNumero, \
            NumFichaFac, NoReversaCV, NumInternoRef in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_notacredito (Numero, Fecha, CodCli, Factura, Neto, Iva, " \
                                               "Total, IngStock, Ila, Glosa_NC, Tipo_Doc, Boleta, CodigoEmp," \
                                               " CorrEmpresa, PorcIVA, PorcILA, NumFolio, TipoDebito, Observaciones," \
                                               " Estado, CodOper, Centralizada, CodigoOC, num_periodo, CentralizaCV, " \
                                               "FechaContabiliza, MotivoAnulacion, CodRefDTE, CDITotal, " \
                                               "CDIIVA, CDIILA, Abonos, Saldo, bSelect, Temp, CodCC, WS_NumCabID," \
                                               " WS_FolioSII, CabLlgId, CabOpeNumero, NumFichaFac, NoReversaCV," \
                                               " NumInternoRef) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Numero, Fecha, CodCli, Factura, Neto, Iva, Total, IngStock, Ila, Glosa_NC, Tipo_Doc,
                   Boleta, CodigoEmp, CorrEmpresa, PorcIVA, PorcILA, NumFolio, TipoDebito, Observaciones,
                   Estado, CodOper, Centralizada, CodigoOC, num_periodo, CentralizaCV, FechaContabiliza,
                   MotivoAnulacion, CodRefDTE, CDITotal, CDIIVA, CDIILA, Abonos, Saldo, bSelect, Temp,
                   CodCC, WS_NumCabID, WS_FolioSII, CabLlgId, CabOpeNumero, NumFichaFac, NoReversaCV, NumInternoRef)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla Notacredito: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'notacredito', 'bdg_notacredito', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA detallenc 28
        i = 0
        kupay_cursor.execute(
            'SELECT NumeroNC, CodProducto, DescripcionNC, CantidadNC, PrecioNC, SubTotalNC,  '
            'Unidad,  MontoImp, Lote, CodBod, Cuenta, CCosto, CalculaILA, CtaCostoVta, '
            'CostoVta, CImputacion FROM detallenc')
        registrosorigen = kupay_cursor.rowcount
        print("(28) tabla detallenc")
        for NumeroNC, CodProducto, DescripcionNC, CantidadNC, PrecioNC, SubTotalNC, Unidad, MontoImp, Lote, \
            CodBod, Cuenta, CCosto, CalculaILA, CtaCostoVta, CostoVta, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_detallenc (NumeroNC, CodProducto, DescripcionNC, CantidadNC, " \
                                               "PrecioNC, SubTotalNC,  Unidad,  MontoImp, Lote, CodBod, Cuenta, " \
                                               "CCosto, CalculaILA, CtaCostoVta, CostoVta, CImputacion) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (NumeroNC, CodProducto, DescripcionNC, CantidadNC, PrecioNC, SubTotalNC, Unidad,
                   MontoImp, Lote, CodBod, Cuenta, CCosto, CalculaILA, CtaCostoVta, CostoVta, CImputacion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla detallenc: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'detallenc', 'bdg_detallenc', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA MonValorDia 29
        i = 0
        kupay_cursor.execute('SELECT CodMon, Fecha, Valor FROM monvalordia where year(fecha)=' + str(AgnoACarga))
        registrosorigen = kupay_cursor.rowcount
        print("(29) tabla MonValorDia")
        for CodMon, Fecha, Valor in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_monvalordia(CodMon, Fecha, Valor) VALUES (%s, %s, %s)"
            val = (CodMon, Fecha, Valor)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla MonValorDia: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'monvalordia', 'bdg_monvalordia', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_notacreditoexp 30
        i = 0
        kupay_cursor.execute(
            'SELECT Numero,  Fecha,  CodCliente,  NumeroFactExp,  FechaFactExp,  FormaPagoCod,  GlosaTotal2,  '
            'GlosaGral,  CorrEmpresa,  CodigoEmp,  TotalValor,  Total2,  TotalGral,  CodMon,  Tipo_Doc,  '
            'ModStock,  TasaMon ,  TasaPesosMon ,  TotalPesos,  TipoDebito,  Observaciones, Estado,  '
            'CodOper,  CodigoOC,  numperiodo,   Centralizada,  CentralizaCV ,  FechaContabiliza,  '
            'ExpresadoEn,  MotivoAnulacion,  CDITotal,  CodRefDTE,  PtoEmbarque,  PtoDestino,  '
            'NumOrdenPO,  EsDeNCE,  CodCC,  WS_NumCabID,  WS_FolioSII,  CabLlgId,  CabOpeNumero FROM NotaCreditoExp')
        registrosorigen = kupay_cursor.rowcount
        print("(30) tabla bdg_notacreditoexp")
        for Numero, Fecha, CodCliente, NumeroFactExp, FechaFactExp, FormaPagoCod, GlosaTotal2, GlosaGral, CorrEmpresa, \
            CodigoEmp, TotalValor, Total2, TotalGral, CodMon, Tipo_Doc, ModStock, TasaMon, TasaPesosMon, TotalPesos, \
            TipoDebito, Observaciones, Estado, CodOper, CodigoOC, numperiodo, Centralizada, CentralizaCV, \
            FechaContabiliza, ExpresadoEn, MotivoAnulacion, CDITotal, CodRefDTE, PtoEmbarque, PtoDestino, \
            NumOrdenPO, EsDeNCE, CodCC, WS_NumCabID, WS_FolioSII, CabLlgId, \
            CabOpeNumero in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_notacreditoexp (Numero,  Fecha,  CodCliente,  NumeroFactExp,  " \
                                               "FechaFactExp,  FormaPagoCod,  GlosaTotal2,  GlosaGral,  " \
                                               "CorrEmpresa,  CodigoEmp,  TotalValor,  Total2,  TotalGral,  " \
                                               "CodMon,  Tipo_Doc,  ModStock,  TasaMon ,  TasaPesosMon ,  " \
                                               "TotalPesos,  TipoDebito,  Observaciones, Estado,  CodOper,  " \
                                               "CodigoOC,  numperiodo,   Centralizada,  CentralizaCV ,  " \
                                               "FechaContabiliza,  ExpresadoEn,  MotivoAnulacion,  CDITotal,  " \
                                               "CodRefDTE,  PtoEmbarque,  PtoDestino,  NumOrdenPO,  EsDeNCE,  CodCC,  " \
                                               "WS_NumCabID,  WS_FolioSII,  CabLlgId,  CabOpeNumero) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (Numero, Fecha, CodCliente, NumeroFactExp, FechaFactExp, FormaPagoCod, GlosaTotal2, GlosaGral,
                   CorrEmpresa, CodigoEmp, TotalValor, Total2, TotalGral, CodMon, Tipo_Doc, ModStock, TasaMon,
                   TasaPesosMon, TotalPesos, TipoDebito, Observaciones, Estado, CodOper, CodigoOC, numperiodo,
                   Centralizada, CentralizaCV, FechaContabiliza, ExpresadoEn, MotivoAnulacion, CDITotal,
                   CodRefDTE, PtoEmbarque, PtoDestino, NumOrdenPO,
                   EsDeNCE, CodCC, WS_NumCabID, WS_FolioSII, CabLlgId, CabOpeNumero)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla notacreditoexp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen,NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'notacreditoexp', 'bdg_notacreditoexp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA DetalleNCExp 31
        i = 0
        kupay_cursor.execute(
            'SELECT Numero, CodProducto, Descripcion, Cantidad,  Precio, ValorTotal, Unidad, Lote,CodBod, NLinea, Cuenta,'
            ' CCosto, ValorTotPesos, CtaCostoVta, EsMuestra, CImputacion FROM DetalleNCExp')
        registrosorigen = kupay_cursor.rowcount
        print("(31) tabla DetalleNCExp")
        for Numero, CodProducto, Descripcion, Cantidad, Precio, ValorTotal, Unidad, Lote, CodBod, NLinea, Cuenta, CCosto, \
            ValorTotPesos, CtaCostoVta, EsMuestra, CImputacion in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_DetalleNCExp (Numero, CodProducto, Descripcion, Cantidad,  Precio, " \
                                               "ValorTotal, Unidad, Lote,CodBod, NLinea, Cuenta, CCosto, ValorTotPesos, " \
                                               "CtaCostoVta, EsMuestra, CImputacion) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (Numero, CodProducto, Descripcion, Cantidad, Precio, ValorTotal, Unidad, Lote, CodBod, NLinea, Cuenta,
                   CCosto, ValorTotPesos, CtaCostoVta, EsMuestra, CImputacion)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla DetalleNCExp: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'DetalleNCExp', 'bdg_DetalleNCExp', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA fami_mat 32
        i = 0
        kupay_cursor.execute(
            'SELECT CodFam,Familia,NItem,CodCtaGenerica,TotCant,TotMonto,TotCantPeriodo,TotMontoPeriodo,TotCantFinal,'
            'TotMontoFinal,Serie,CorrInicio,CorrFin,CorrActual,NoVigente, FechaEstado,UltUsuarioMod,CodInterno,'
            'CodCtaGastos FROM fami_mat')
        registrosorigen = kupay_cursor.rowcount
        print("(32) tabla bdg_fami_mat")
        for CodFam, Familia, NItem, CodCtaGenerica, TotCant, TotMonto, TotCantPeriodo, TotMontoPeriodo, TotCantFinal, \
            TotMontoFinal, Serie, CorrInicio, CorrFin, CorrActual, NoVigente, FechaEstado, UltUsuarioMod, CodInterno, \
            CodCtaGastos in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_fami_mat (CodFam,Familia,NItem,CodCtaGenerica,TotCant,TotMonto," \
                                               "TotCantPeriodo,TotMontoPeriodo,TotCantFinal,TotMontoFinal,Serie," \
                                               "CorrInicio,CorrFin,CorrActual,NoVigente, FechaEstado,UltUsuarioMod," \
                                               "CodInterno,CodCtaGastos) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodFam, Familia, NItem, CodCtaGenerica, TotCant, TotMonto, TotCantPeriodo, TotMontoPeriodo, TotCantFinal,
                   TotMontoFinal, Serie, CorrInicio, CorrFin, CorrActual, NoVigente, FechaEstado, UltUsuarioMod, CodInterno,
                   CodCtaGastos)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_fami_mat: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'fami_mat', 'bdg_fami_mat', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA precios_cliente 33
        i = 0
        kupay_cursor.execute(
            'select codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, montodes,'
            ' fechaestado, ultusuariomod from precios_cliente')
        registrosorigen = kupay_cursor.rowcount
        print("(33) tabla bdg_precios_cliente")
        for codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, montodes, fechaestado, \
            ultusuariomod in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_precios_cliente (codcliente, codproducto, unidad, codmon, " \
                                               "precio, codclientenac, codconcepto, porc, montodes, fechaestado, " \
                                               "ultusuariomod) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (codcliente, codproducto, unidad, codmon, precio, codclientenac, codconcepto, porc, montodes, fechaestado,
                   ultusuariomod)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_precios_cliente: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'precios_cliente', 'bdg_precios_cliente', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_movi_insumos 34
        i = 0
        kupay_cursor.execute(
            'select id_movi, fecha, ndoc, cant, producto, ingreso, egreso, valor, documento, saldo, id_vc, valorpmp, '
            'saldopesos, codbod, montoingreso, montoegreso, tipo_doc, fechareal '
            ' from movi_insumos where year(fecha) >= ' + str(AgnoAnteriorCarga))
        registrosorigen = kupay_cursor.rowcount
        print("(34) tabla bdg_movi_insumos")
        for id_movi, fecha, ndoc, cant, producto, ingreso, egreso, valor, documento, saldo, id_vc, valorpmp, saldopesos, \
            codbod, montoingreso, montoegreso, tipo_doc, fechareal in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_movi_insumos (id_movi, fecha, ndoc, cant, producto, ingreso, egreso," \
                                               "valor, documento, saldo, id_vc, valorpmp, saldopesos, codbod, " \
                                               "montoingreso, montoegreso, tipo_doc, fechareal) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                                               " %s, %s, %s, %s, %s, %s, %s)"
            val = (id_movi, fecha, ndoc, cant, producto, ingreso, egreso, valor, documento,
                   saldo, id_vc, valorpmp, saldopesos, codbod, montoingreso, montoegreso, tipo_doc, fechareal)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_movi_insumos: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'movi_insumos', 'bdg_movi_insumos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_bodegas 35
        i = 0
        kupay_cursor.execute(
            'select codigo, bodega, ubicacion, mapabod_, tipo, costostd, web, codplanta, novigente, fechaestado, '
            'ultusuariomod, esptovta from bodegas')
        registrosorigen = kupay_cursor.rowcount
        print("(35) tabla bdg_bodegas")
        for codigo, bodega, ubicacion, mapabod_, tipo, costostd, web, codplanta, novigente, fechaestado, ultusuariomod, \
            esptovta in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_bodegas(codigo, bodega, ubicacion, mapabod_, tipo, costostd, web, " \
                                               "codplanta, novigente, fechaestado, ultusuariomod, esptovta) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (codigo, bodega, ubicacion, mapabod_, tipo, costostd, web, codplanta, novigente, fechaestado,
                   ultusuariomod, esptovta)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_bodegas: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'bodegas', 'bdg_bodegas', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_insumos 36
        i = 0
        kupay_cursor.execute(
            "select codigo, tipo, valorpmp, unidad, insumo, aplicacion, factor, existencia, total, lote, solucion, "
            "tipoaplicacion, minimo, codigo_ext, codctactble, corrcmonet, noenologico, ficha_, cantidadinicial, "
            "cantidadfinal, totmontofinal, errorkardex, tomainv, codctamerma, ccostomerma, novigente, fechaestado, "
            "ultusuariomod, codctagastos, tmpvalorpmp from insumos where codigo<>'TAN005'")
        registrosorigen = kupay_cursor.rowcount
        print("(36) tabla bdg_insumos")
        for codigo, tipo, valorpmp, unidad, insumo, aplicacion, factor, existencia, total, lote, solucion, tipoaplicacion, \
            minimo, codigo_ext, codctactble, corrcmonet, noenologico, ficha_, cantidadinicial, cantidadfinal, \
            totmontofinal, errorkardex, tomainv, codctamerma, ccostomerma, novigente, fechaestado, ultusuariomod, \
            codctagastos, tmpvalorpmp in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_insumos(codigo, tipo, valorpmp, unidad, insumo, aplicacion, " \
                                               "factor, existencia, total, lote, solucion, tipoaplicacion, minimo, " \
                                               "codigo_ext, codctactble, corrcmonet, noenologico, ficha_, " \
                                               "cantidadinicial, cantidadfinal, totmontofinal, errorkardex, tomainv, " \
                                               "codctamerma, ccostomerma, novigente, fechaestado, ultusuariomod, " \
                                               "codctagastos, tmpvalorpmp) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (codigo, tipo, valorpmp, unidad, insumo, aplicacion, factor, existencia, total, lote,
                   solucion, tipoaplicacion, minimo, codigo_ext, codctactble, corrcmonet, noenologico,
                   ficha_, cantidadinicial, cantidadfinal, totmontofinal, errorkardex, tomainv, codctamerma,
                   ccostomerma, novigente, fechaestado, ultusuariomod, codctagastos, tmpvalorpmp)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_insumos: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'insumos', 'bdg_insumos', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_condicionpago 37
        kupay_cursor.execute("SELECT codcondpago, nomcondpago, tipocondpago, fin700codigo,"
                             " diascondpago, diahabilsgte  FROM condicionpago")
        registrosorigen = kupay_cursor.rowcount
        print("(37) tabla bdg_condicionpago")
        i = 0
        for codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte in kupay_cursor.fetchall():
            i = i + 1
            print(codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte)
            sql = "INSERT INTO " + EsquemaBD + ".bdg_condicionpago(codcondpago, nomcondpago, tipocondpago, " \
                                               "fin700codigo, diascondpago, diahabilsgte) VALUES (%s, %s, %s, %s, %s, %s)"
            val = (codcondpago, nomcondpago, tipocondpago, fin700codigo, diascondpago, diahabilsgte)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_condicionpago: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, " \
                                           "TablaDestino, NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'condicionpago', 'bdg_condicionpago', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Muestra fecha y hora actual al finalizar el proceso
        localtime2 = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de finalizacion del proceso")
        print(localtime2)

        # Truncacion de fecha carga
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".FechaCargaInformacion")

        # Registro de fecha cargada en base de datos
        Proceso = 'P01'
        Descripcion = 'Carga Panel Comercial'
        fecha = time.localtime(time.time())
        sql = "INSERT INTO " + EsquemaBD + ".FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
        val = (Proceso, Descripcion, fecha)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Cierre de cursores y bases de datos
        kupay_cursor.close()
        kupay.close()
        bdg.close()
        bdg_cursor.close()
        print("fin cierre de cursores y bases")
    envio_mail("Fin proceso de Cargar Tablas")


if __name__ == "__main__":
    main()
