import sqlite3
from sqlite3 import Error

#creating database connection

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return
    : Connection object or None
    """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print("Database created succesfully")
    except Error as e:
        print(e)

    return conn

#creating table
def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        print("table created")
    except Error as e:
        print(e)


# create  table name 'movi'
def create_movie(conn,movie):
       
    """
    Create a new movie
    :param conn:
    :param movie:
    :return:
    """
    
    
    sql = '''INSERT OR REPLACE INTO movi(mov_nm,actor_nm,actress_nm,director_nm,release_year)VALUES(?,?,?,?,?)'''
    cur = conn.cursor()
    cur.execute(sql,movie)
    conn.commit()
    
    return cur.lastrowid

#Below function retrieves all data from table(like SELECT * method)

def select_all_movies(conn):

    cur = conn.cursor()
    cur.execute("SELECT * FROM movi")

    rows = cur.fetchall()

    for row in rows:
        print(row)

#Below function retrieve data from table according to our condition(like SELECT * from TABLE WHERE..)

def select_task_by_priority(conn, mov_nm):
    """
    Query tasks by mov_nm
    :param conn: the Connection object
    :param mov_nm:
    :return:
    """
    cur = conn.cursor()
    cur.execute("SELECT * FROM movi WHERE mov_nm=?", (mov_nm,))

    rows = cur.fetchall()

    for row in rows:
        print(row)

#main function

def main():
    database = r"C:\sqlite\db\filmy.db"
    
    # create a database connection
    conn = create_connection(database)

    sql_create_movie_table = """ CREATE TABLE IF NOT EXISTS movi (
                                       
                                        mov_nm text PRIMARY KEY,
                                        actor_nm text ,
                                        actress_nm text,
                                        director_nm text NOT NULL,
                                        release_year text
                                    ); """

    if conn is not None:
        create_table(conn, sql_create_movie_table)
    else:
        print("Error! cannot create the database connection.")

    
    with conn:
       
        # movies
        movie_1 = ('Sultan', 'salmankhan', 'anushkasharma', 'alizafar', '2016')
        movie_2 = ('Dangal', 'AmirKhan', 'FatimaShaikh', 'NiteshTiwari','2016')
        movie_3 = ('Dhoom','AbhishekhBacchan','IshaDeol','VijayAcharya','2004')
        # data insertion in table
        create_movie(conn,movie_1)
        create_movie(conn,movie_2)
        create_movie(conn,movie_3)

        #below statement print element of table  by our priority(like SELECT method)
        print("1.Show data by movie name: ")
       
       #we are retrieving data by MOVIE NAME: 'DHOOM'
        select_task_by_priority(conn, 'Dhoom')
        
        #below statement print all elements present in table
        print("2. Show all data from TABLE: ")
        select_all_movies(conn)

if __name__ == '__main__':
    main()
