import sqlite3
from sqlite3 import Error

import random

def create_connection(db_file = ":memory:"):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    conn = None

    try:
        conn = sqlite3.connect(db_file)
    except Error as e:
        print(e)
    finally:
        return conn

def create_table(conn, create_table_sql):
    """ create a table from the create_table_sql statement
    :param conn: Connection object
    :param create_table_sql: a CREATE TABLE statement
    :return:
    """
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
        conn.commit()
    except Error as e:
        print(e)

def create_artist(conn, artist):
    """
    Create a new artist into the Artists table
    :param conn: Connection object
    :param artist: tuple with artist info
    :return: artist id
    """
    sql = ''' INSERT INTO artists(ArtistId, Name)
              VALUES(?,?) '''
    cur = conn.cursor()
    cur.execute(sql, artist)
    conn.commit()
    return cur.lastrowid

def create_all_tables(conn, *statements):
    """
    Create as many tables as demanded
    :param conn: Connection object
    :param statements: one or more SQL commands for table creation
    :return:
    """
    if conn is not None:
        for sql in statements:
            #TODO: check the sql statements are really for creating tables
            create_table(conn, sql)
    else:
        print("Error! cannot create the database connection.")

def get_schema(conn, table):
    """
    Retrieves the table schema info
    :param conn: Connection object
    :param table: name of the table to retrieve information from
    :return: table schema or None
    """
    if conn is not None:
        sql = f"PRAGMA table_info({table})"
        c = conn.cursor()
        table_info = c.execute(sql).fetchall()

        for row in table_info:
            print(row)
        return table_info
    else:
        print("Error! Connection to the database is invalid.")
        return None
    
    # Schema returns as a tuple in the form of:
    # (Row_Id, Row_name, Data type, if column can be NULL, default value, primary key index)

def table_eda(conn, table_name, table_info):
    """
    Retrieves the table characteritics like value limits and sample values
    :param conn: Connection object
    :param table_name: name of the table to retrieve information from
    :param table_info: schema of the table to guide data generation
    :return: table info necessary for data generation
    """
    #get the number of rows
    sql = f"SELECT count(*) FROM {table_name}"
    c = conn.cursor()
    n_rows = c.execute(sql).fetchall()

    print(n_rows)

    table_eda_list = []

    #get min and max values for each number collumn
    print("Info for", table_name)
    for row in table_info:
        if row[2] == 'INTEGER':
            sql = f"SELECT min({row[1]}), max({row[1]}) FROM {table_name}"
            minmax = c.execute(sql).fetchall()
            print("Row:",row[1], " - ", minmax[0])

            table_eda_list.append((row[1], row[2], minmax[0]))

        else:   #for nvarchar
            sql = f"SELECT {row[1]} FROM {table_name} LIMIT 10"
            sample_strings = c.execute(sql).fetchall()
            print("Row:",row[1], " - ", sample_strings)

            table_eda_list.append((row[1], row[2], sample_strings))
    
    return table_eda_list

def generate_data(table_name, table_info, n_items):
    """
    Retrieves the table characteritics like value limits and sample values
    :param table_name: name of the table to retrieve information from
    :param table_info: basic EDA of the table to guide data generation
    :param n_items: amount of rows to generate data
    :return: list with generated data tuples
    """
    item_list = []

    for _1 in range(n_items):

        temp_item = []

        for item in table_info:
            if item[1] == 'INTEGER':
                minimum = item[2][0]
                maximum = item[2][1]

                temp_item.append(random.randint(minimum,maximum))
            
            else:
                random_pick = random.choice(item[2])
                temp_item.append(random_pick[0][:random.randint(0, 20)])
        
        item_list.append(tuple(temp_item))
    
    return item_list



def main():

    #database file name to connect to. use raw string if providing full path
    database = r"C:\sqlite\sample_db\chinook.db"
    generated_database =  r"C:\sqlite\sample_db\chinook_test.db"

    # create a database connection
    conn = create_connection(database)

    # create tables
    #create_all_tables(conn, sql_create_projects_table, sql_create_tasks_table)

    #get schema from tables
    table = "artists"

    table_schema = get_schema(conn, table)

    #get detailed information on table data
    table_info = table_eda(conn, table, table_schema)
    print(table_info)

    new_data = generate_data(table, table_schema, 20)

    #if new_data is not None:
        # create_generated_db(generated_database, table, new_data)
        #gen_conn = create_connection(generated_database)


    conn.close()

if __name__ == '__main__':
    main()
