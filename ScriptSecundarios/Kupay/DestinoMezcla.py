import sys
import pyodbc
import pymysql
import time
from datetime import datetime
from DatosConexion.VG import IPServidor, UsuarioBD, PasswordBD

def main():
    # VariablesGlobales
    EsquemaBD = "stagekupay"
    SistemaOrigen = "Kupay"
    fechacarga = datetime.now()

    # Inicializar variables locales
    now = datetime.now()
    AgnoACarga = now.year
    MesDeCarga = now.month
    AgnoAnteriorCarga = AgnoACarga - 1

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
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_destino")
        bdg_cursor.execute("TRUNCATE TABLE " + EsquemaBD + ".bdg_destinomezcla")
        bdg_cursor.execute("COMMIT;")
        print("Fin del proceso de truncado de tablas en " + EsquemaBD + "")

        # Muestra fecha y hora actual al iniciar el proceso
        localtime = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de inicio del proceso")
        print(localtime)

        ######################
        # Cargar tablas
        ######################
        # TABLA bdg_destino 44
        i = 0
        kupay_cursor.execute(
            "select Num_ODB, Codigo_Ap, Cuba_Des, TipoVino_Ap, Existencia_Ap,  Existencia_Pp,  Codigo_Pp,  LtsReal, "
            " TipoVino_Pp,  NomCubaDes,  CodInt_Vino,  NumItem, NomVari,  CodVino,  NBarricas,  Est_Final, Cap_Cuba, "
            "EstadoAp, EstadoPp, Hecha, LtsOrigen, CaliDes, Cos_Pp,  Cos_Ap,  LtsDescubeTinto, LitrosMovimiento, "
            "KilosCuba_Ap,  KilosCuba_Pp, Grado,  KilosOrigen,  NOrden,  Recargada,  CAST(tcostoexisteal  AS FLOAT) "
            "FROM destino ")

        registrosorigen = kupay_cursor.rowcount
        print("(44) tabla bdg_destino")
        for Num_ODB, Codigo_Ap, Cuba_Des, TipoVino_Ap, Existencia_Ap, Existencia_Pp, Codigo_Pp, LtsReal, TipoVino_Pp, \
            NomCubaDes, CodInt_Vino, NumItem, NomVari, CodVino, NBarricas, Est_Final, Cap_Cuba, EstadoAp, EstadoPp, \
            Hecha, LtsOrigen, CaliDes, Cos_Pp, Cos_Ap, LtsDescubeTinto, LitrosMovimiento, KilosCuba_Ap, KilosCuba_Pp, \
             Grado, KilosOrigen, NOrden, Recargada, TCostoExisteAl in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_destino (Num_ODB, Codigo_Ap, Cuba_Des, TipoVino_Ap, Existencia_Ap, " \
                                               "Existencia_Pp,  Codigo_Pp,  LtsReal,  TipoVino_Pp,  NomCubaDes, " \
                                               "CodInt_Vino,  NumItem, NomVari,  CodVino,  NBarricas,  Est_Final," \
                                               "Cap_Cuba, EstadoAp, EstadoPp, Hecha, LtsOrigen, CaliDes, Cos_Pp,  " \
                                               "Cos_Ap,  LtsDescubeTinto, LitrosMovimiento, KilosCuba_Ap,  KilosCuba_Pp, " \
                                               "Grado,  KilosOrigen,  NOrden,  Recargada,  TCostoExisteAl) " \
                                               "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                               "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            val = (Num_ODB, Codigo_Ap, Cuba_Des, TipoVino_Ap, Existencia_Ap, Existencia_Pp, Codigo_Pp, LtsReal, TipoVino_Pp,
                   NomCubaDes, CodInt_Vino, NumItem, NomVari, CodVino, NBarricas, Est_Final, Cap_Cuba, EstadoAp, EstadoPp,
                   Hecha, LtsOrigen, CaliDes, Cos_Pp, Cos_Ap, LtsDescubeTinto, LitrosMovimiento, KilosCuba_Ap, KilosCuba_Pp,
                   Grado, KilosOrigen, NOrden, Recargada, TCostoExisteAl)
            print(val)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_destino: ", i)

        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'destino', 'bdg_destino', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # TABLA bdg_destinomezcla 45
        i = 0
        kupay_cursor.execute("select Codvino, Codigo, Cantidad, Temp1, Cosecha FROM destinomezcla ")

        registrosorigen = kupay_cursor.rowcount
        print("(45) tabla bdg_destinomezcla")
        for Codvino, Codigo, Cantidad, Temp1, Cosecha in kupay_cursor.fetchall():
            i = i + 1
            sql = "INSERT INTO " + EsquemaBD + ".bdg_destinomezcla (Codvino, Codigo, Cantidad, Temp1, Cosecha) " \
                                               "VALUES (%s, %s, %s, %s, %s)"
            val = (Codvino, Codigo, Cantidad, Temp1, Cosecha)
            bdg_cursor.execute(sql, val)
            bdg.commit()
        print("Cantidad de registros en la tabla bdg_destinomezcla: ", i)
        # Proceso cuadratura de carga
        sql = "INSERT INTO " + EsquemaBD + ".proc_cuadratura (id, SistemaOrigen, TablaOrigen, TablaDestino, " \
                                           "NroRegistroOrigen, NroRegistroDestino, FechaCarga) " \
                                           "VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (Identificador, SistemaOrigen, 'destinomezcla', 'bdg_destinomezcla', registrosorigen, i, fechacarga)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Muestra fecha y hora actual al finalizar el proceso
        localtime2 = time.asctime(time.localtime(time.time()))
        print("Fecha y hora de finalizacion del proceso")
        print(localtime2)

        # Registro de fecha cargada en base de datos
        Proceso = 'P02'
        Descripcion = 'Carga Balance'
        fecha = time.localtime(time.time())
        sql = "INSERT INTO FechaCargaInformacion (proceso, descripcion,fecha) VALUES (%s, %s, %s)"
        val = (Proceso, Descripcion, fecha)
        bdg_cursor.execute(sql, val)
        bdg.commit()

        # Cierre de cursores y bases de datos
        kupay_cursor.close()
        kupay.close()
        bdg.close()
        bdg_cursor.close()
        print("fin cierre de cursores y bases")

if __name__ == "__main__":
    main()