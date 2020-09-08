#!/usr/bin/env python


import os
import csv
import sqlite3

from config import config



script_path = os.path.dirname(os.path.realpath(__file__))


config_path_name = os.path.join(script_path, 'config2.ini')
db = config('db', config_path_name)
dataset = config('dataset', config_path_name)


schema_path_name = os.path.join(script_path, db['schema'])


def create_schema():

    conn = sqlite3.connect(db['database'])

    
    c = conn.cursor()

   
    c.executescript(open(schema_path_name, "r").read())

    
    conn.commit()

   
    conn.close()










def fill(chunksize=2):
    
    with open(dataset['autor']) as fi:
        reader = csv.DictReader(fi)
        

        for row in reader:
            insert_autor(row['autor'])

   
    with open(dataset['libro']) as fi:
        reader = csv.DictReader(fi)
        chunk = []

        for row in reader: 
            items = [row['titulo'], int(row['cantidad_paginas']), row['autor']]
            chunk.append(items)
            if len(chunk) == chunksize:
                insert_grupo(chunk)
                chunk.clear()
        
        if chunk:
            insert_grupo(chunk)
            
               


def insert_autor( name):
    conn = sqlite3.connect(db['database'])
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    c.execute("""
        INSERT INTO autor (author)
        VALUES (?);""", (name,))

    conn.commit()
    
    conn.close()

def insert_grupo(group):
    conn = sqlite3.connect(db['database'])
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    
    c.executemany("""
            INSERT INTO libro (title, pags, fk_author_id)
            SELECT ?,?, a.id
            FROM autor as a
            WHERE a.author =?;""", group)

   

    conn.commit()
    
    conn.close()

def fetch(id):
    
    conn = sqlite3.connect(db['database'])
    conn.execute("PRAGMA foreign_keys = 1")
    c = conn.cursor()

    c.execute("""SELECT l.id, l.title, l.pags, a.author
                 FROM libro AS l, autor AS a
                 WHERE l.fk_author_id = a.id;""")

    while True:
        row = c.fetchone()
        if row is None:
            break
        elif id == 0:
            print(row)
        elif row[0] == id:
            print(row)
        

    # Cerrar la conexión con la base de datos
    conn.close()



def search_author(book_title):

    conn = sqlite3.connect(db['database'])
    c = conn.cursor()
    c.execute('SELECT * FROM libro')
   
    c.execute("""SELECT l.id, l.title, l.pags, a.author
                 FROM libro AS l, autor AS a
                 WHERE l.fk_author_id = a.id;""")

    while True:
        row = c.fetchone()
        if row is None:
            break
        
        elif row[1] == book_title:
            return(row[3])

                 

                 

        



if __name__ == "__main__":
  
  create_schema()

  
  fill()

  
  fetch(0)  
  fetch(3) 
  fetch(20)  

  
  
  print(search_author('Relato de un naufrago'))