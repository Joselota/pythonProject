# Funciones Globales

def f_limpiar(parametro):
    if str(parametro) == 'inf':
        parametro = 0
    if str(parametro) == '-inf':
        parametro = 0
    return parametro


def f_SQLEsc(s):
    if s == None:
        return "NULL"
    else:
        return s

def SQLEsc(s):
    if s == None:
        return "NULL"
    if len(str(s)) == 0:
        return "NULL"
    if s == "None":
        return "NULL"
    else:
        return s

