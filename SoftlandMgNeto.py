import pymysql
import time
import pyodbc as pyodbc
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD
from DatosConexion.VG import DRIVER, SERVER, DATABASE, UID, PWD

# VariablesGlobales
EsquemaBD = "cgf"
periodo = 2024

# Muestra fecha y hora actual al iniciar el proceso
localtime = time.asctime(time.localtime(time.time()))
print("Fecha y hora de inicio del proceso")
print(localtime)

# Base de datos de Gestion (donde se cargaran los datos)
bdg = pymysql.connect(host=IPServidor, port=3306, user=UsuarioBD, passwd=PasswordBD, db=EsquemaBD)
bdg_cursor = bdg.cursor()

print("Inicio de proceso de borrado de tablas ")
print("DELETE FROM cgf.tab_factcg where CpbAno="+str(periodo))
bdg_cursor.execute("DELETE FROM "+str(EsquemaBD)+".tab_factcg where CpbAno="+str(periodo))
print("Fin de proceso de borrado de tablas ")

# Base de datos Softland (Desde donde se leen los datos)
stringConn: str = (
        'Driver={' + DRIVER + '};SERVER=' + SERVER + ';DATABASE=' + DATABASE + ';UID=' + UID + ';PWD=' + PWD + ';')
# print(stringConn)
lista = [5000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]
i = 0
try:
    conn = pyodbc.connect(stringConn)
    cursor = conn.cursor()
    sql = "SELECT cwpctas.PCCODI, cwpctas.PCDESC, cwcpbte.CpbFec, cwcpbte.CpbNum, cwcpbte.CpbTip, cwcpbte.CpbNui, " \
          "cwmovim.CajCod, cwmovim.CcCod, cwmovim.CodAux, cwmovim.TtdCod, cwmovim.NumDoc, cwmovim.DgaCod, " \
          "cwmovim.IfCod, cwmovim.MovDebe, cwmovim.MovHaber, cwmovim.MovGlosa, cwcpbte.cpbglo, cwcpbte.CpbEst, " \
          "cwcpbte.CpbAno, cwtdetg.desdet " \
          "FROM softland.cwmovim cwmovim " \
          "join softland.cwcpbte cwcpbte	ON (cwmovim.CpbAno = cwcpbte.CpbAno	AND cwmovim.CpbNum = cwcpbte.CpbNum	" \
          "AND cwmovim.AreaCod = cwcpbte.AreaCod) " \
          "join softland.cwpctas cwpctas	ON (cwmovim.PctCod = cwpctas.PCCODI) " \
          "left join softland.cwtdetg cwtdetg	ON (cwtdetg.coddet = cwmovim.DgaCod) " \
          "WHERE cwcpbte.CpbEst in ('V','N') " \
          " AND cwcpbte.CpbAno="+str(periodo)
    print(sql)
    cursor.execute(sql)
    records = cursor.fetchall()
    print("Total registros leidos desde Softland: ", len(records))
    i = 0
    for x in records:
        i = i + 1
        PCCODI = x[0]
        PCDESC = x[1]
        CpbFec = x[2]
        CpbNum = x[3]
        CpbTip = x[4]
        CpbNui = x[5]
        CajCod = x[6]
        CcCod = x[7]
        CodAux = x[8]
        TtdCod = x[9]
        NumDoc = x[10]
        DgaCod = x[11]
        IfCod = x[12]
        MovDebe = x[13]
        MovHaber = x[14]
        MovGlosa = x[15]
        cpbglo = x[16]
        CpbEst = x[17]
        CpbAno = x[18]
        desdet = x[19]
        sql = "insert into cgf.tab_factcg (PCCODI, PCDESC, CpbFec, CpbNum, CpbTip, CpbNui, CajCod, CcCod," \
              "CodAux, TtdCod, NumDoc, DgaCod, IfCod, MovDebe, MovHaber, MovGlosa, cpbglo, CpbEst, CpbAno, desdet)" \
              " VALUES (%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,%s, %s, %s,  %s, %s)"
        val = (PCCODI, PCDESC, CpbFec, CpbNum, CpbTip, CpbNui, CajCod, CcCod, CodAux, TtdCod, NumDoc,
               DgaCod, IfCod, MovDebe, MovHaber, MovGlosa, cpbglo, CpbEst, CpbAno, desdet)
        print(val)
        bdg_cursor.execute(sql, val)
        bdg.commit()
        if i in lista:
            print("Se han cargados : " + str(i))
    # Cerrar la conexión a softland
    cursor.close()
    conn.close()

except pyodbc.DataError as err:
    print("An exception occurred")
    print(err)

print("Total registros cargados a DL :" + str(i))

print("Ejecutando Procesamiento almacenado que determina Margen Neto (proc_MargenNeto)")
bdg_cursor.execute("call stagecomercial.proc_MargenNeto;")
print("Termino de proc_MargenNeto")


# Registro de fecha cargada en base de datos
Proceso = 'P01'
Descripcion = 'Carga Softland'
fecha = time.localtime(time.time())
sql = "INSERT INTO cgf.FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
val = (Proceso, Descripcion, fecha)
bdg_cursor.execute(sql, val)
bdg.commit()

# Cerrar la conexión del datalake
bdg_cursor.close()
bdg.close()
