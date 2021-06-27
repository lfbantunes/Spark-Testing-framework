from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, ForeignKey, select, insert
import random
import configparser

def get_schema(engine, meta):
    """
    get the existing tables info from the database into the metadata object
    :param engine:
    :param meta:
    :return: void
    """
    meta.reflect(engine)

    print('these are your tables:')
    for t in meta.sorted_tables:
        print(t.name, end='; ')
    print('no more tables')

def create_all_tables(engine, meta, tables):
    """
    Create as many tables as demanded
    :param engine:
    :param meta:
    :param tables: list of tables to create
    :return:
    """
    ignored_tables = ["sqlite_"]

    for t in tables:
        if not t.name.startswith(tuple(ignored_tables)):
            t.create(bind=engine, checkfirst=True)

def get_tables(engine, meta, tables=None):
    """
    Retrieves the tables objects
    :param conn: Connection object
    :param table: name of the table to retrieve information from
    :return: list of tables
    """
    sorted_tables = meta.sorted_tables
    min_index = -1

    sorted_tables_names = [t.name for t in sorted_tables]

    for t in tables:
        try:
            t_index = sorted_tables_names.index(t)
            if t_index > min_index:
                min_index = t_index
        except ValueError:
            print(f"Table {t} doesn't exist in this database.")

    if min_index == -1:
        min_index = len(sorted_tables_names)
    else:
        min_index += 1

    all_tables = []

    #print(sorted_tables_names[:min_index])

    for t in sorted_tables_names[:min_index]:
        table = Table(t, meta, autoload_with=engine)
        all_tables.append(table)
    
    return all_tables


def generate_data(origin, destiny, table, n_items=10):
    """
    where generation models will be called, for now only copies
    :param table_name: name of the table to retrieve information from
    :param table_info: basic EDA of the table to guide data generation
    :param n_items: amount of rows to generate data
    :return: list with generated data tuples
    """
    print('copying data')

    select_stmt = select(table)
    #insert_stmt = insert(table).from_select(select_stmt)

    for ct in table.foreign_key_constraints:
        print(ct.columns[0] , ct.columns[0].foreign_keys)
        # print(ct.columns[0])
        # print(ct)

    with destiny.begin() as n_conn, origin.begin() as conn:
        result = conn.execute(select_stmt).fetchmany(10)
        
        for row in result:
            try:
                n_conn.execute(table.insert(), row._mapping)
            except Exception as e:
                #print("Error on row insertion:")
                print(type(e))
                #print(e)


    print('copying ended')

def main():

    config = configparser.ConfigParser()
    config.read('config.ini')

    #database file name to connect to. use raw string if providing full path
    #setup path in config file
    database_uri = config['DEFAULT']['origin_database_uri']
    generated_database_uri = config['DEFAULT']['destiny_database_uri']

    # create sqlalchemy engine and metadata objects
    engine = create_engine(database_uri)
    meta = MetaData()

    #get updated tables to metadata
    get_schema(engine, meta)

    # list of tables to generate
    gen_tables_names = ['film']

    # retrieve table objects
    tables = get_tables(engine, meta, gen_tables_names)

    #create clone tables
    new_engine = create_engine(generated_database_uri, echo=False) #turn true for help debugging
    new_meta = MetaData(new_engine)
    get_schema(new_engine, new_meta)

    create_all_tables(new_engine, new_meta, tables)

    get_schema(new_engine, new_meta)
    
    for t in new_meta.sorted_tables[:5]:
        generate_data(engine, new_engine, t)


if __name__ == '__main__':
    main()
