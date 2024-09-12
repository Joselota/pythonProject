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
relative_url = "/sites/Inventarios/Documentos compartidos/Inventario/Inventario Casillero/csv_auto.csv"

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
#print(download_path)
df = pd.read_csv(download_path, delimiter=',', keep_default_na=False)
df = df.reset_index()
#print(df)
#for index, row in df.iterrows():
#    print(index, row)

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
        cursor.execute("TRUNCATE " + EsquemaBD + ".inventario")
        record = cursor.fetchone()

        # loop dataframe
        for i, row in df.iterrows():
            sql = "INSERT INTO " + EsquemaBD + ".inventario (`cant. De botellas envasadas`,Lote, fecha," \
                                               "`Tipo Vino`,`CALIDAD`,`cosecha`,`Botella`,Corcho," \
                                               "`Capacidad`,`TotalFisicoBot`, `Total Litros físico`," \
                                               "`Fecha verificación stock`,`Qty bins cmp`,`Bot acum`,`# bins`," \
                                               "`saldo`,`total bot`,`Observaciones`,`TotalReservadobot`," \
                                               "`Total Reservado Litros.`,`Disponible Bot`)  " \
                                               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                                               "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
            val = (row["cant_de_botellas"], row["lote"], row["fecha"],
                   row["tipo_vino"], row["calidad"], row["cosecha"], row["botella"], row["corcho"],
                   row["capacidad"], row["total_bot_fisico"], row["total_litros_fisico"],
                   row["fecha_verificacion_stock"], row["qty_bins_cmp"], row["bot_acum"], row["#_bins"],
                   row["saldo"], row["total_bot"], row["observaciones"], row["total_reservado_bot"],
                   row["total_reservado_litros"], row["disponible_bot"])
            #print(sql, val)
            cursor.execute(sql, val)
            conn.commit()

        print("cantidad de registros agregados: " + str(i + 1) + ", tabla Inventarios")
except Error as e:
    print("Error mientras conectaba a MySql", e)
