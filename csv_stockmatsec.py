import pandas as pd
import mysql.connector as mysql
from mysql.connector import Error
from office365.runtime.auth.authentication_context import AuthenticationContext
from office365.sharepoint.client_context import ClientContext
import os
import tempfile
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD, usernameSP, passwordSP

# VariablesGlobales
EsquemaBD = "stagekupay"

# Setup conexion a sharepoint y a csv online
url = "https://viumanent.sharepoint.com/sites/Inventarios"
relative_url = "/sites/Inventarios/Documentos compartidos/Inventario/Inventarios Materia Seca/csv_auto.csv"

# conexion y apertura
ctx_auth = AuthenticationContext(url)
if ctx_auth.acquire_token_for_user(usernameSP, passwordSP):
    ctx = ClientContext(url, ctx_auth)
    web = ctx.web
    ctx.load(web)
    ctx.execute_query()
    print("Conectado a : {0}".format(web.properties['Title']))
else:
    print(ctx_auth.get_last_error())

download_path = os.path.join(tempfile.mkdtemp(), os.path.basename(relative_url))
with open(download_path, "wb") as local_file:
    file = ctx.web.get_file_by_server_relative_path(relative_url).download(local_file).execute_query()

print("[Ok] se ha descargado el archivo en: {0}".format(download_path))

# lectura CSV
df = pd.read_csv(download_path, delimiter=',', keep_default_na=False)
# print(df)

# Conexion a base de datos
try:
    conn = mysql.connect(host=IPServidor, user=UsuarioBD, password=PasswordBD)

    if conn.is_connected():
        cursor = conn.cursor()
        print("Conectado a la base de datos...")
except Error as e:
    print("Error mientras conectaba a MySql", e)

# Conexion a tabla, insercion de datos obtenidos de csv
try:
    conn = mysql.connect(host=IPServidor, database=EsquemaBD, user=UsuarioBD, password=PasswordBD)
    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("TRUNCATE " + EsquemaBD + ".`stockmatsec`;")
        record = cursor.fetchone()

        # loop dataframe
        for i, row in df.iterrows():
            # print(tuple(row))
            sql = "INSERT INTO " + EsquemaBD + ".stockmatsec(" \
                  "`FAMILIA`,`GRUPO`,`CODIGO`,`DESCRIPCION`,`Stock en bodega`,`Stock según registros`,`Obsoleto`,`No conforme`," \
                  "`Stock Disponible`,`Requerimiento`,`Disponible-Requerimiento`,`COSTO $`,`TOTAL $`,`KRAFT`,`COLOR`," \
                  "`Observación`,`Tabique`,`Reservas`,`Forescast MKT`,`S.mín  MKT Nacional`) " \
                  " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            if row[6] == "":
                row[6] = 0
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("cantidad de registros agregados : " + str(i + 1) + ", en stockmatsec")
except Error as e:
    print("Error mientras conectaba a MySql", e)
