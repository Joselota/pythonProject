import pymysql
import time
import pyodbc as pyodbc
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, sender_email, email_pass, receiver_email, email_smtp
import smtplib
from email.message import EmailMessage
from DatosConexion.VG import DRIVER, SERVER, DATABASE, UID, PWD
from datetime import datetime

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

def main():
    # VariablesGlobales
    EsquemaBD = "stagesoftland"
    periodo = 2024
    # Inicializar variables locales
    #now = datetime.now()
    #periodo = now.year
    print(periodo)

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    # Base de datos de Gestion (donde se cargaran los datos)
    bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
    bdg_cursor = bdg.cursor()

    print("Inicio de proceso de borrado de tablas ")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.cwtauxi")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.CWPCTAS")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.CWDocSaldos")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.cwttdoc")
    # bdg_cursor.execute("TRUNCATE TABLE stagesoftland.cwcpbte")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.cwtdetg")
    bdg_cursor.execute("TRUNCATE TABLE stagesoftland.cwtccos")
    bdg_cursor.execute("DELETE FROM stagesoftland.cwcpbte where cpbano="+str(periodo))
    bdg_cursor.execute("DELETE FROM stagesoftland.cwdetli where cpbano="+str(periodo))
    bdg_cursor.execute("DELETE FROM stagesoftland.cwmovim where cpbano="+str(periodo))

    print("Fin de proceso de borrado de tablas ")

    # Base de datos Softland (Desde donde se leen los datos)
    stringConn: str = (
            'Driver={' + DRIVER + '};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + UID + ';PWD=' + PWD + ';')
    #print(stringConn)
    i = 0
    try:
        conn = pyodbc.connect(stringConn)
        cursor = conn.cursor()

        # cargar tabla cwtdetg
        sql = "SELECT coddet, desdet, niveldet FROM softland.cwtdetg"
        cursor.execute(sql)
        i = 0
        for coddet, desdet, niveldet in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwtdetg (coddet, desdet, niveldet)" \
                  " VALUES (%s, %s, %s )"
            val = (coddet, desdet, niveldet)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros cargados cwtdetg tabla a DL :" + str(i))

        # cargar tabla CWTCCOS
        sql = "SELECT CodiCC, DescCC, NivelCc FROM softland.CWTCCOS where len(DescCC)>0 AND DescCC<>'No Utilizar' "
        cursor.execute(sql)
        i = 0
        for CodiCC, DescCC, NivelCc in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.CWTCCOS (CodiCC, DescCC, NivelCc)" \
                  " VALUES (%s, %s, %s )"
            val = (CodiCC, DescCC, NivelCc)
            bdg_cursor.execute(sql, val)
            bdg.commit()
            # print(CodiCC, DescCC, NivelCc)
        print("Total registros cargados tabla CWTCCOS a DL :" + str(i))

        # Cargar tabla cwcpbte
        sql = "SELECT CpbAno, CpbNum, AreaCod, CpbFec, CpbMes, CpbEst, CpbTip, CpbNui, " \
              "CpbGlo, CpbImp, CpbCon, Sistema, Proceso, Usuario, CpbNormaIFRS, CpbNormaTrib, " \
              "CpbAnoRev, CpbNumRev, SistemaMod, ProcesoMod, FechaUlMod, TipoLog FROM softland.cwcpbte where CpbAno="+str(periodo)
        cursor.execute(sql)
        i = 0
        for CpbAno, CpbNum, AreaCod, CpbFec, CpbMes, CpbEst, CpbTip, CpbNui, CpbGlo, \
            CpbImp, CpbCon, Sistema, Proceso, Usuario, CpbNormaIFRS, CpbNormaTrib, \
            CpbAnoRev, CpbNumRev, SistemaMod, ProcesoMod, FechaUlMod, TipoLog in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwcpbte (CpbAno, CpbNum, AreaCod, CpbFec, CpbMes," \
                  " CpbEst, CpbTip, CpbNui, CpbGlo, CpbImp, CpbCon, Sistema, Proceso, Usuario, " \
                  "CpbNormaIFRS, CpbNormaTrib, CpbAnoRev, CpbNumRev, SistemaMod, ProcesoMod, " \
                  "FechaUlMod, TipoLog)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
            val = (CpbAno, CpbNum, AreaCod, CpbFec, CpbMes, CpbEst, CpbTip, CpbNui,
                   CpbGlo, CpbImp, CpbCon, Sistema, Proceso, Usuario, CpbNormaIFRS,
                   CpbNormaTrib, CpbAnoRev, CpbNumRev, SistemaMod, ProcesoMod, FechaUlMod, TipoLog)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros cargados tabla cwcpbte a DL :" + str(i))

        # # ########
        # cargar tabla de clientes y empresas cwtauxi
        sql = "select CodAux, NomAux, actaux, rutAux, noFaux, dirAux,FonAux1, " \
              "FonAux2, Fonaux3,ClaCli, cladis, claemp, claotr from softland.cwtauxi"
        print(sql)
        cursor.execute(sql)
        # records = cursor.fetchall()
        i = 0
        for CodAux, NomAux, actaux, rutAux, noFaux, dirAux, FonAux1, FonAux2, Fonaux3, ClaCli, cladis, \
            claemp, claotr in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwtauxi (CodAux, NomAux, actaux, rutAux, noFaux, dirAux,FonAux1, " \
                  "FonAux2, Fonaux3,ClaCli, cladis, claemp, claotr)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CodAux, NomAux, actaux, rutAux, noFaux, dirAux, FonAux1, FonAux2,
                   Fonaux3, ClaCli, cladis, claemp, claotr)
            # print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tabla cwtauxi cargados a DL :" + str(i))

        # cargar tabla de cuentas contables CWPCTAS
        sql = "select PCCODI, PCNIVEL, PCLNIVEL, PCDESC, PCTIPO,PCCCOS, PCAUXI,PCCDOC, PCEDOC,PCCONB, " \
              "PCMONE,PCDETG, PCPREC,PCEPRC, PCIFIN,PCCOMON, PCTPCM,PCCAPP, PCACTI,PCCMON, PCCODC,PCDINBA, " \
              "PCCMCP,PCIDMA, PCCBADICI,PCAjusteDifC, PCFijaMonBase,PCAfeEfe, PCConEfe,PCEfeSVS " \
              "from softland.CWPCTAS"
        # print(sql)
        cursor.execute(sql)
        # records = cursor.fetchall()
        i = 0
        for PCCODI, PCNIVEL, PCLNIVEL, PCDESC, PCTIPO, PCCCOS, PCAUXI, PCCDOC, PCEDOC, PCCONB, PCMONE, PCDETG, PCPREC, \
            PCEPRC, PCIFIN, PCCOMON, PCTPCM, PCCAPP, PCACTI, PCCMON, PCCODC, PCDINBA, PCCMCP, PCIDMA, PCCBADICI, \
            PCAjusteDifC, PCFijaMonBase, PCAfeEfe, PCConEfe, PCEfeSVS in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.CWPCTAS (PCCODI, PCNIVEL, PCLNIVEL, PCDESC, PCTIPO,PCCCOS, PCAUXI,PCCDOC, " \
                  "PCEDOC,PCCONB, PCMONE,PCDETG, PCPREC,PCEPRC, PCIFIN,PCCOMON, PCTPCM,PCCAPP, PCACTI,PCCMON, " \
                  "PCCODC,PCDINBA, PCCMCP,PCIDMA, PCCBADICI,PCAjusteDifC, PCFijaMonBase,PCAfeEfe, PCConEfe,PCEfeSVS)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                  "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (PCCODI, PCNIVEL, PCLNIVEL, PCDESC, PCTIPO, PCCCOS, PCAUXI, PCCDOC, PCEDOC, PCCONB, PCMONE, PCDETG,
                   PCPREC, PCEPRC, PCIFIN, PCCOMON, PCTPCM, PCCAPP, PCACTI, PCCMON, PCCODC, PCDINBA, PCCMCP, PCIDMA,
                   PCCBADICI, PCAjusteDifC, PCFijaMonBase, PCAfeEfe, PCConEfe, PCEfeSVS)
            # print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tabla CWPCTAS cargados a DL :" + str(i))

        # cargar tabla de cuentas contables cwdetli
        sql = "select cpbano, cpbnum, movnum, areacod, ttdcod, dlindoc, CONVERT(VARCHAR(20), dlifedoc, 120) as dlifedoc, " \
              "codaux, dlicoint, " \
              "dlimto01, dlimto02, dlimto03, dlimto04, dlimto05, dlimto06, dlimto07, dlimto08, dlimto09, dlimto10, " \
              "CONVERT(VARCHAR(20), cpbfec, 120) as cpbfec, moncod, monto, " \
              "movequiv, detldesde, detlhasta, documentonulo, lotedespacho, dlimtoad01, dlimtoad02, dlimtoad03, " \
              "dlimtoad04, dlimtoad05, dlimtoad06, dlimtoad07, dlimtoad08, dlimtoad09, dlimtoad10, dliequiv01, " \
              "dliequiv02, dliequiv03, dliequiv04, dliequiv05, dliequiv06, dliequiv07, dliequiv08, dliequiv09, " \
              "dliequiv10, libromonedabase from softland.cwdetli WHERE cwdetli.cpbano="+str(periodo)
        # print(sql)
        cursor.execute(sql)
        i = 0
        for cpbano, cpbnum, movnum, areacod, ttdcod, dlindoc, dlifedoc, codaux, dlicoint, dlimto01, dlimto02, dlimto03, \
            dlimto04, dlimto05, dlimto06, dlimto07, dlimto08, dlimto09, dlimto10, cpbfec, moncod, monto, movequiv,  \
            detldesde, detlhasta, documentonulo, lotedespacho, dlimtoad01, dlimtoad02, dlimtoad03, dlimtoad04, dlimtoad05,\
            dlimtoad06, dlimtoad07, dlimtoad08, dlimtoad09, dlimtoad10, dliequiv01, dliequiv02, dliequiv03, dliequiv04,  \
            dliequiv05, dliequiv06, dliequiv07, dliequiv08, dliequiv09, dliequiv10, libromonedabase in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwdetli (cpbano, cpbnum, movnum, areacod, ttdcod, dlindoc, dlifedoc, " \
                  "codaux, dlicoint, dlimto01, dlimto02, dlimto03, dlimto04, dlimto05, dlimto06, dlimto07, dlimto08, " \
                  "dlimto09, dlimto10, cpbfec, moncod, monto, movequiv, detldesde, detlhasta, documentonulo, " \
                  "lotedespacho, dlimtoad01, dlimtoad02, dlimtoad03, dlimtoad04, dlimtoad05, dlimtoad06, " \
                  "dlimtoad07, dlimtoad08, dlimtoad09, dlimtoad10, dliequiv01, dliequiv02, dliequiv03, " \
                  "dliequiv04, dliequiv05, dliequiv06, dliequiv07, dliequiv08, dliequiv09, dliequiv10, libromonedabase)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (cpbano, cpbnum, movnum, areacod, ttdcod, dlindoc, dlifedoc, codaux, dlicoint, dlimto01, dlimto02,
                   dlimto03, dlimto04, dlimto05, dlimto06, dlimto07, dlimto08, dlimto09, dlimto10, cpbfec, moncod, monto,
                   movequiv, detldesde, detlhasta, documentonulo, lotedespacho, dlimtoad01, dlimtoad02, dlimtoad03,
                   dlimtoad04, dlimtoad05, dlimtoad06, dlimtoad07, dlimtoad08, dlimtoad09, dlimtoad10, dliequiv01,
                   dliequiv02, dliequiv03, dliequiv04, dliequiv05, dliequiv06, dliequiv07, dliequiv08, dliequiv09,
                   dliequiv10, libromonedabase)
            # print(sql, val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tabla cwdetli cargados a DL :" + str(i))

        # cargar tabla de cuentas contables cwmovim
        sql = "select CpbAno, CpbNum, MovNum, AreaCod, PctCod, CpbFec, CpbMes, CvCod, VendCod, UbicCod, CajCod, " \
              "IfCod, MovIfCant, DgaCod, MovDgCant, CcCod, TipDocCb, NumDocCb, CodAux, TtdCod, NumDoc, MovFe, MovFv, " \
              "MovTipDocRef, MovNumDocRef, MovDebe, MovHaber, MovGlosa, MonCod, MovEquiv, MovDebeMa, MovHaberMa, " \
              "MovNumCar, MovTC, MovNC, MovIPr, MovAEquiv, FecPag, CODCPAG, CbaNumMov, CbaAnoC, GrabaDLib, CpbOri, " \
              "CodBanco, CodCtaCte, MtoTotal, Cuota, CuotaRef, Marca, fecEmisionch, paguesea, Impreso,dlicoint_aperturas," \
              "nro_operacion, FormadePag, CpbNormaIFRS, CpbNormaTrib " \
              "from softland.cwmovim  where CpbAno="+str(periodo)
        # print(sql)
        cursor.execute(sql)
        i = 0
        for CpbAno, CpbNum, MovNum, AreaCod, PctCod, CpbFec, CpbMes, CvCod, VendCod, UbicCod, CajCod, IfCod, MovIfCant, \
            DgaCod, MovDgCant, CcCod, TipDocCb, NumDocCb, CodAux, TtdCod, NumDoc, MovFe, MovFv, MovTipDocRef, MovNumDocRef,\
            MovDebe, MovHaber, MovGlosa, MonCod, MovEquiv, MovDebeMa, MovHaberMa, MovNumCar, MovTC, MovNC, MovIPr, \
            MovAEquiv, FecPag, CODCPAG, CbaNumMov, CbaAnoC, GrabaDLib, CpbOri, CodBanco, CodCtaCte, MtoTotal, Cuota, \
            CuotaRef, Marca, fecEmisionch, paguesea, Impreso, dlicoint_aperturas, nro_operacion, FormadePag, \
            CpbNormaIFRS, CpbNormaTrib in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwmovim (CpbAno, CpbNum, MovNum, AreaCod, PctCod, CpbFec, CpbMes, CvCod, " \
                  "VendCod, UbicCod, CajCod, IfCod, MovIfCant, DgaCod, MovDgCant, CcCod, TipDocCb, NumDocCb, CodAux, " \
                  "TtdCod, NumDoc, MovFe, MovFv, MovTipDocRef, MovNumDocRef, MovDebe, MovHaber, MovGlosa, MonCod,MovEquiv,"\
                  " MovDebeMa, MovHaberMa, MovNumCar, MovTC, MovNC, MovIPr, MovAEquiv, FecPag, CODCPAG, CbaNumMov," \
                  " CbaAnoC, GrabaDLib, CpbOri, CodBanco, CodCtaCte, MtoTotal, Cuota, CuotaRef, Marca, fecEmisionch, " \
                  "paguesea, Impreso, dlicoint_aperturas, nro_operacion, FormadePag, CpbNormaIFRS, CpbNormaTrib)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (CpbAno, CpbNum, MovNum, AreaCod, PctCod, CpbFec, CpbMes, CvCod, VendCod, UbicCod, CajCod, IfCod,
                   MovIfCant, DgaCod, MovDgCant, CcCod, TipDocCb, NumDocCb, CodAux, TtdCod, NumDoc, MovFe, MovFv,
                   MovTipDocRef, MovNumDocRef, MovDebe, MovHaber, MovGlosa, MonCod, MovEquiv, MovDebeMa, MovHaberMa,
                   MovNumCar, MovTC, MovNC, MovIPr, MovAEquiv, FecPag, CODCPAG, CbaNumMov, CbaAnoC, GrabaDLib, CpbOri,
                   CodBanco, CodCtaCte, MtoTotal, Cuota, CuotaRef, Marca, fecEmisionch, paguesea, Impreso,
                   dlicoint_aperturas, nro_operacion, FormadePag, CpbNormaIFRS, CpbNormaTrib)
            # print(sql, val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tabla cwmovim cargados a DL :" + str(i))

        # cargar tabla de cuentas contables CWDocSaldos
        sql = "select movtipdocref, movnumdocref, cuotaref, codaux, pctcod, debe, haber, debema, haberma " \
              "from softland.CWDocSaldos"
        # print(sql)
        cursor.execute(sql)
        i = 0
        for movtipdocref, movnumdocref, cuotaref, codaux, pctcod, debe, haber, debema, haberma in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.CWDocSaldos (movtipdocref, movnumdocref, cuotaref, codaux, " \
                  "pctcod, debe, haber, debema, haberma)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (movtipdocref, movnumdocref, cuotaref, codaux, pctcod, debe, haber, debema, haberma)
            # print(sql, val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tabla CWDocSaldos cargados a DL :" + str(i))

        # cargar tabla de cuentas contables cwttdoc
        sql = "SELECT coddoc, desdoc, ctlubi, libdoc, libret, valorret, libope, duping, duplic, ingreso, " \
              "genletra, impumonto, ctacteban, duptdnumfec, aplicalote, ctacontdef, dtedocsii, dupsmis, " \
              "lcvsii, dtedocsiidef, avisocont, diasaviso, tipovalidacionaviso, imgdocto, origenimgdocto, " \
              "formatoimgdocto, colorimgdocto, resimgdocto, manejainfadidoc, controlaauxv, dispositivochk, " \
              "iddispositivochk, usuario, proceso  FROM softland.cwttdoc"
        # print(sql)
        cursor.execute(sql)
        i = 0
        for coddoc, desdoc, ctlubi, libdoc, libret, valorret, libope, duping, duplic, ingreso, genletra,  \
            impumonto, ctacteban, duptdnumfec, aplicalote, ctacontdef, dtedocsii, dupsmis, lcvsii,  \
            dtedocsiidef, avisocont, diasaviso, tipovalidacionaviso, imgdocto, origenimgdocto, \
            formatoimgdocto, colorimgdocto, resimgdocto, manejainfadidoc, controlaauxv, dispositivochk, iddispositivochk, \
            usuario, proceso in cursor.fetchall():
            i = i + 1
            sql = "insert into stagesoftland.cwttdoc (coddoc, desdoc, ctlubi, libdoc, libret, valorret, libope, " \
                  "duping, duplic, ingreso, genletra, impumonto, ctacteban, duptdnumfec, aplicalote, ctacontdef, " \
                  "dtedocsii, dupsmis, lcvsii, dtedocsiidef, avisocont, diasaviso, tipovalidacionaviso, imgdocto, " \
                  "origenimgdocto, formatoimgdocto, colorimgdocto, resimgdocto, manejainfadidoc, controlaauxv, " \
                  "dispositivochk, iddispositivochk, usuario, proceso)" \
                  " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s," \
                  " %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s , %s)"
            val = (coddoc, desdoc, ctlubi, libdoc, libret, valorret, libope, duping, duplic, ingreso, genletra,
                   impumonto, ctacteban, duptdnumfec, aplicalote, ctacontdef, dtedocsii, dupsmis, lcvsii,
                   dtedocsiidef, avisocont, diasaviso, tipovalidacionaviso, imgdocto, origenimgdocto,
                   formatoimgdocto, colorimgdocto, resimgdocto, manejainfadidoc, controlaauxv, dispositivochk,
                   iddispositivochk, usuario, proceso)
            # print(sql, val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Total registros tablas cwttdoc cargados a DL :" + str(i))
        # Cerrar la conexión a softland
        cursor.close()
        conn.close()


    except pyodbc.DataError as err:
        print("An exception occurred")
        print(err)

    # Truncacion de fecha carga
    bdg_cursor.execute("TRUNCATE TABLE "+EsquemaBD+".FechaCargaInformacion")

    # Registro de fecha cargada en base de datos
    Proceso = 'P01'
    Descripcion = 'Carga Softland'
    fecha = time.localtime(time.time())
    sql = "INSERT INTO stagesoftland.FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
    val = (Proceso, Descripcion, fecha)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Cerrar la conexión del datalake
    bdg_cursor.close()
    bdg.close()

    # Envio de mail con aviso de termino de ejecución script
    envio_mail("Aviso fin ejecución script Softland en DL")
    exit(1)

if __name__ == "__main__":
    main()