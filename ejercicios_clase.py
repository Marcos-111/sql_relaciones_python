#!/usr/bin/env python
'''
SQL Introducción [Python]
Ejercicios de clase
---------------------------
Autor: Inove Coding School
Version: 1.1

Descripcion:
Programa creado para poner a prueba los conocimientos
adquiridos durante la clase
'''

__author__ = "Inove Coding School"
__email__ = "alumnos@inove.com.ar"
__version__ = "1.1"

import sqlite3

# https://extendsclass.com/sqlite-browser.html


def create_schema():

    # Conectarnos a la base de datos
    # En caso de que no exista el archivo se genera
    # como una base de datos vacia
    conn = sqlite3.connect('secundaria.db')

    # Crear el cursor para poder ejecutar las querys
    c = conn.cursor()

    # Ejecutar una query
    c.execute("""
                DROP TABLE IF EXISTS estudiante;
            """)

    c.execute("""
            DROP TABLE IF EXISTS tutor;
        """)

    # Ejecutar una query
    c.execute("""
        CREATE TABLE tutor(
            [id] INTEGER PRIMARY KEY AUTOINCREMENT,
            [name] TEXT NOT NULL
        );
        """)

    c.execute("""
            CREATE TABLE estudiante(
                [id] INTEGER PRIMARY KEY AUTOINCREMENT,
                [name] TEXT NOT NULL,
                [age] INTEGER NOT NULL,
                [grade] INTEGER NOT NULL,
                [fk_tutor_id] INTEGER NOT NULL REFERENCES tutor(id)
            );
            """)

    # Para salvar los cambios realizados en la DB debemos
    # ejecutar el commit, NO olvidarse de este paso!
    conn.commit()

    # Cerrar la conexión con la base de datos
    conn.close()


def fill():
    print('Completemos esta tablita!')
    # Llenar la tabla de la secundaria con al munos 2 tutores
    # Cada tutor tiene los campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del tutor (puede ser solo nombre sin apellido)

    # Llenar la tabla de la secundaria con al menos 5 estudiantes
    # Cada estudiante tiene los posibles campos:
    # id --> este campo es auto incremental por lo que no deberá completarlo
    # name --> El nombre del estudiante (puede ser solo nombre sin apellido)
    # age --> cuantos años tiene el estudiante
    # grade --> en que año de la secundaria se encuentra (1-6)
    # fk_tutor_id --> id de su tutor

    # Se debe utilizar la sentencia INSERT.
    # Observar que todos los campos son obligatorios
    # Cuando se insert los los estudiantes sería recomendable
    # que utilice el INSERT + SELECT para que sea más legible
    # el INSERT del estudiante con el nombre del tutor

    # No olvidarse que antes de poder insertar un estudiante debe haberse
    # primero insertado el tutor.
    # No olvidar activar las foreign_keys!


def insert_persona_grupo(group):
    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    try:
        c.executemany("""
            INSERT INTO estudiante (name, age, grade, fk_tutor_id)
            SELECT ?,?,?, t.id
            FROM tutor as t
            WHERE t.name =?;""", group)
    except sqlite3.Error as err:
        print(err)

    conn.commit()
    
    conn.close()


def insert_tutor( name):
    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    c.execute("""
        INSERT INTO tutor (name)
        VALUES (?);""", (name,))

    conn.commit()
    
    conn.close()


def fetch():
    print('Comprovemos su contenido, ¿qué hay en la tabla?')
    # Utilizar la sentencia SELECT para imprimir en pantalla
    # todas las filas con todas sus columnas de la tabla estudiante.
    # No debe imprimir el id del tutor, debe reemplazar el id por el nombre
    # del tutor en la query, utilizando el concepto de INNER JOIN,
    # se puede usar el WHERE en vez del INNER JOIN.
    # Utilizar fetchone para imprimir de una fila a la vez

    # columnas que deben aparecer en el print:
    # id / name / age / grade / tutor_nombre

    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    # Leer todas las filas y obtener los datos de a uno
    c.execute("""SELECT e.id, e.name, e.age, e.grade, t.name
                 FROM estudiante as e, tutor as t
                 WHERE e.fk_tutor_id = t.id;""")

    
    while True:
        row = c.fetchone()
        if row is None:
            break
        print(row)

    # Cerrar la conexión con la base de datos
    conn.close()


def search_by_tutor(tutor):
    print('Operación búsqueda!')
    # Esta función recibe como parámetro el nombre de un posible tutor.
    # Utilizar la sentencia SELECT para imprimir en pantalla
    # aquellos estudiantes que tengan asignado dicho tutor.

    # De la lista de esos estudiantes el SELECT solo debe traer
    # las siguientes columnas por fila encontrada:
    # id / name / age / tutor_nombre

    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    
    c.execute("""SELECT e.id, e.name, e.age, e.grade, t.name
                 FROM estudiante as e, tutor as t
                 WHERE e.fk_tutor_id = t.id;""")

    
    
    while True:

        row = c.fetchone()
        if row is None:
            break
        if row[4] == tutor:
            print(row)
        else:
            pass
    
        
           

    conn.close()

    


def modify(id, name):
    print('Modificando la tabla')
    # Utilizar la sentencia UPDATE para modificar aquella fila (estudiante)
    # cuyo id sea el "id" pasado como parámetro,
    # modificar el tutor asignado (fk_tutor_id --> id) por aquel que coincida
    # con el nombre del tutor pasado como parámetro

    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    c.execute("""UPDATE estudiante
                            SET fk_tutor_id =
                            (SELECT t.id FROM tutor as t
                             WHERE t.name =?)
                            WHERE id =?""",
                         (name, id))

    

    # Save
    conn.commit()
    # Cerrar la conexión con la base de datos
    conn.close()


def count_grade(grade):
    print('Estudiante por grado')
    # Utilizar la sentencia COUNT para contar cuantos estudiante
    # se encuentran cursando el grado "grade" pasado como parámetro
    # Imprimir en pantalla el resultado


        
    conn = sqlite3.connect('secundaria.db')
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

   
    c.execute("""SELECT COUNT(e.grade) AS grade_count
                 FROM estudiante as e
                 WHERE e.grade = ?;""", (grade,))

    result = c.fetchone()
    count = result[0]
    print('Estudiantes en grado', grade, 'encontrados:', count)


    # Cerrar la conexión con la base de datos
    conn.close()


if __name__ == '__main__':
    print("Bienvenidos a otra clase de Inove con Python")
    create_schema()   # create and reset database (DB)
    fill()
    

    insert_tutor('Ignacio')
    insert_tutor('Miranda')
    insert_tutor('Lucrecia')


    group = [('Max', 13, 2, 'Miranda' ),
              ('Joao', 15, 4, 'Ignacio'),
              ('Micaela', 13, 2, 'Miranda'),
              ('Celeste', 17, 6, 'Lucrecia')
              ]

    insert_persona_grupo(group)

    fetch()

    tutor = 'Miranda'
    search_by_tutor(tutor)

    
    modify(1, 'Lucrecia')

    grade = 2
    count_grade(grade)
