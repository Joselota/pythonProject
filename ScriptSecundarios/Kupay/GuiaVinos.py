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
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_detalle_guiauva")
    bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_vino_guias")
    print("Fin del proceso de truncado de tablas en " + EsquemaBD + " ")

    # Muestra fecha y hora actual al iniciar el proceso
    localtime = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de inicio del proceso")
    print(localtime)

    ######################
    # Cargar tablas
    ######################

    # TABLA bdg_guia_despacho 56
    i = 0
    kupay_cursor.execute("select CodGuia, Cod_Parc_Prod, CodVar, KilosGuia, Valor, Grado, KgGrado, LtsGota, LtsPrensa, LtsTot, Grad_Base, "
                         "BSelec, CubFech, LtsEst, AT, AzucarRed, CubaGota, CubaPrensa, TEnv, CapacEnv, NEnvases, GradoTabla, Prens, Temp, "
                         "Densidad, Temp_Corr, Dens_Corr, GotaIn, PrenIn, TMol, Tipo_Cosecha, CodVino, TotalValor, Castigo, KgPago, Saldo, "
                         "PH, DifValor, FCamPrecio, CambiaGuia, LitrosCorregidos, Brix, NDetalle, Calidad, PesoRacimo, KilosDescarte, "
                         "KilosRomana, MontoDifValor, Centralizada, CabOpeNumero from detalle_guiauva;")
    registrosorigen = kupay_cursor.rowcount
    print("(60) tabla bdg_detalle_guiauva")
    for CodGuia, Cod_Parc_Prod, CodVar, KilosGuia, Valor, Grado, KgGrado, LtsGota, LtsPrensa, LtsTot, Grad_Base,  \
        BSelec, CubFech, LtsEst, AT, AzucarRed, CubaGota, CubaPrensa, TEnv, CapacEnv, NEnvases, GradoTabla, Prens, Temp,  \
        Densidad, Temp_Corr, Dens_Corr, GotaIn, PrenIn, TMol, Tipo_Cosecha, CodVino, TotalValor, Castigo, KgPago, Saldo,  \
        PH, DifValor, FCamPrecio, CambiaGuia, LitrosCorregidos, Brix, NDetalle, Calidad, PesoRacimo, KilosDescarte,  \
        KilosRomana, MontoDifValor, Centralizada, CabOpeNumero in kupay_cursor.fetchall():
       i = i+1
       sql = "INSERT INTO " + EsquemaBD + ".bdg_detalle_guiauva(CodGuia, Cod_Parc_Prod, CodVar, KilosGuia, Valor, Grado, KgGrado, " \
                                          "LtsGota, LtsPrensa, LtsTot, Grad_Base, BSelec, CubFech, LtsEst, AT, AzucarRed, CubaGota, " \
                                          "CubaPrensa, TEnv, CapacEnv, NEnvases, GradoTabla, Prens, Temp, Densidad, Temp_Corr, " \
                                          "Dens_Corr, GotaIn, PrenIn, TMol, Tipo_Cosecha, CodVino, TotalValor, Castigo, KgPago, " \
                                          "Saldo, PH, DifValor, FCamPrecio, CambiaGuia, LitrosCorregidos, Brix, NDetalle, Calidad, " \
                                          "PesoRacimo, KilosDescarte, KilosRomana, MontoDifValor, Centralizada, CabOpeNumero) " \
                                          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                          "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
       val = (CodGuia, Cod_Parc_Prod, CodVar, KilosGuia, Valor, Grado, KgGrado, LtsGota, LtsPrensa, LtsTot, Grad_Base,    \
              BSelec, CubFech, LtsEst, AT, AzucarRed, CubaGota, CubaPrensa, TEnv, CapacEnv, NEnvases, GradoTabla, Prens, Temp,  \
              Densidad, Temp_Corr, Dens_Corr, GotaIn, PrenIn, TMol, Tipo_Cosecha, CodVino, TotalValor, Castigo, KgPago, Saldo,  \
              PH, DifValor, FCamPrecio, CambiaGuia, LitrosCorregidos, Brix, NDetalle, Calidad, PesoRacimo, KilosDescarte,  \
              KilosRomana, MontoDifValor, Centralizada, CabOpeNumero)
       bdg_cursor.execute(sql, val)
       bdg.commit()
    print("Cantidad de registros en la tabla bdg_detalle_guiauva: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'detalle_guiauva', 'bdg_detalle_guiauva', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # TABLA bdg_apelacion 57
    i = 0
    kupay_cursor.execute("select CodVinoCierre, CodGuia, TMol, CubaHoy from vino_guias")
    registrosorigen = kupay_cursor.rowcount
    print("(61) tabla bdg_vino_guias")
    for CodVinoCierre, CodGuia, TMol, CubaHoy in kupay_cursor.fetchall():
        i = i + 1
        sql = "INSERT INTO " + EsquemaBD + ".bdg_vino_guias (CodVinoCierre, CodGuia, TMol, CubaHoy) VALUES (%s, %s, %s, %s)"
        val = (CodVinoCierre, CodGuia, TMol, CubaHoy)
        bdg_cursor.execute(sql, val)
        bdg.commit()
    print("Cantidad de registros en la tabla bdg_vino_guias: ", i)
    # Proceso cuadratura de carga
    sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                       "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                       "VALUES (%s, %s, %s, %s, %s, %s, %s)"
    val = (Identificador, SistemaOrigen, 'vino_guias', 'bdg_vino_guias', registrosorigen, i, fechacarga)
    bdg_cursor.execute(sql, val)
    bdg.commit()

    # Muestra fecha y hora actual al finalizar el proceso
    localtime2 = time.asctime(time.localtime(time.time()))
    print("Fecha y hora de finalizacion del proceso")
    print(localtime2)

    # Cierre de cursores y bases de datos
    kupay_cursor.close()
    kupay.close()
    bdg_cursor.close()
    bdg.close()
    print("fin cierre de cursores y bases")