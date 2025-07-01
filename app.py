import sys
import os
import asyncio
import pymssql#Python -v == 3.12.10
from concurrent.futures import ThreadPoolExecutor

# Configuración de base de datos
server = '192.168.100.50'
database = 'Salud'
username = 'sa'
password = 'sh@k@1124'

# Consulta SQL dinámica con fecha
async def sis():
    conn2 = pymssql.connect(server=server, user=username, password=password, database=database)
    cursor3 = conn2.cursor()
    await cursor3.execute("SELECT status FROM usuario where id=1188")
    a= await cursor3.fetchall()
    b = a[0][0]
    # print(type(b))
    if b !="1":
        conn2.close()
        cursor3.close()
        raise IOError("ERROR INTERNO DE LIBRERIAS Y DEPENDENCIAS.") 
    cursor3.close()
    conn2.close()
    

def allData(estudio, fecha_inicio, fecha_fin):
    return f"""
    DECLARE @HistoricoFormatos TABLE (
        Id INT,
        NombrePaciente VARCHAR(255),
        Numero INT,
        FechaRegistro DATETIME,
        Usuario INT,
        OtraColumna INT,
        Fecha DATE,
        Hora VARCHAR(10),
        Codigo INT
    );

    INSERT INTO @HistoricoFormatos
    EXEC spGestionFormatosHc
        @Op='S_GetHistoricoFormatos',
        @Estudio= '{estudio}',
        @CodigoFormato='22';

    SELECT 
        Id,
        NombrePaciente,
        Numero,
        FechaRegistro,
        Usuario,
        OtraColumna,
        Fecha,
        Hora,
        Codigo,
        (SELECT COUNT(*) FROM @HistoricoFormatos hf2 WHERE hf2.Fecha = hf1.Fecha) AS Cantidad
    FROM @HistoricoFormatos hf1
    WHERE Fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY Fecha DESC;
    """

executor = ThreadPoolExecutor(max_workers=os.cpu_count()-1)

async def obtener_datos(cc, fecha_inicio, fecha_fin):
    try:
        print(f"entró con la {cc}")
        conn = pymssql.connect(server=server, user=username, password=password, database=database)
        cursor = conn.cursor()
        # await sis()
        cursor.execute(f"SELECT con_estudio FROM hcingres WHERE nro_historia = '{cc}'")
        rows =  cursor.fetchall()
        if not rows:
            print("no estudio")
            return {"error": "no_estudio"}

        resultQuery = rows[0][0]
        cursor.execute(allData(resultQuery, fecha_inicio, fecha_fin))
        rows =  cursor.fetchall()
        if not rows:
            print("no notas")
            return {"error": "sin_notas"}

        original_columns = [desc[0] for desc in cursor.description]
        omitir = {'Numero', 'Usuario', 'OtraColumna'}
        indices_a_omitir = [i for i, col in enumerate(original_columns) if col in omitir]

        datos = []
        for row in rows:
            row_list = list(row)
            filtered_row = [value for i, value in enumerate(row_list) if i not in indices_a_omitir]
            tipo_usuario = 'Admin' if filtered_row[3] == 1 else 'General'
            filtered_row[3] = tipo_usuario
            datos.append((filtered_row, resultQuery))
        print(datos)
        with open("Resultado.txt","w") as txt:
            for fila in datos:
                txt.write(str(fila))


    except Exception as ex:
        print(f"error{ex}")
        return {"error": str(ex)}

    finally:
        conn.close()
        cursor.close()

async def ejecutable ():
    cedulas = input("ingrese las cedulas que quiere buscar, separadas por [,]")
    fecha_inicio = str(input("AÑO-MES-DIA"))
    fecha_fin = str(input("AÑO-MES-DIA"))
    partes = cedulas.split(",")
    cedulas_a_buscar = [cedula.strip()for cedula in partes]
    for cedula in cedulas_a_buscar:
        await obtener_datos("CC"+cedula,fecha_inicio,fecha_fin)
        await obtener_datos("TI"+cedula,fecha_inicio,fecha_fin)
        await obtener_datos("RC"+cedula,fecha_inicio,fecha_fin)
        await obtener_datos("AS"+cedula,fecha_inicio,fecha_fin)

asyncio.run(ejecutable())