#KSENIA_SHUBERT_305

import paramiko
import psycopg2
import csv
import os
import time

hostname = '192.168.122.133'
username = 'postgres'
password = 'P@ssw0rd'
database = 'lab_03'



conn = None

source_dir = 'data/measurement'
target_dir = '/home/postgres/data/data/measurement'

def verify_database_info():
    global hostname, username, password, database, hostname_sftp, username_sftp, password_sftp
    
    print("Current info about connection:")
    print(f"host: {hostname}")
    print(f"user: {username}")
    print(f"password: {password}")
    print(f"database: {database}")
    
    choice = input("its correct data for connection? (1 - yes, 2 - no): ")
    
    if choice == "2":
        hostname = input("host: ")
        username = input("user: ")
        password = input("password: ")
        database = input("database: ")



def create_scheme(scheme_name):
    create_schema_query = f"CREATE SCHEMA IF NOT EXISTS {scheme_name};"
    cursor = conn.cursor()
    cursor.execute(create_schema_query)
    conn.commit()
    cursor.close()

def install_file_fdw(schema):
    cursor = conn.cursor()
    cursor.execute(f"CREATE EXTENSION IF NOT EXISTS file_fdw WITH SCHEMA {schema};")
    conn.commit()
    cursor.close()

def create_file_server(schema):
    cursor = conn.cursor()

    drop_server_query = "DROP SERVER IF EXISTS file_server CASCADE"
    cursor.execute(drop_server_query)
    conn.commit()

    create_server_query = f"""
        CREATE SERVER file_server
        FOREIGN DATA WRAPPER file_fdw;
    """
    cursor.execute(create_server_query)
    conn.commit()
    cursor.close()


def create_foreign_table(table_name, csv_file):
    cursor = conn.cursor()
    create_scheme('external')
    cursor.execute(f"DROP FOREIGN TABLE IF EXISTS external.{table_name}")
    create_table_query = f"""
        CREATE FOREIGN TABLE external.{table_name} (
            city INTEGER,
            mark TIMESTAMP WITHOUT TIME ZONE,
            temperature DOUBLE PRECISION
        )
        SERVER file_server
        OPTIONS (
            filename '{csv_file}',
            format 'csv',
            header 'true'
        );
    """
    cursor.execute(create_table_query)
    conn.commit()
    cursor.close()


def create_measurement_foreign_tables():
    measurements_dir = 'data/measurement' 


    measurement_files = [f for f in os.listdir(measurements_dir) if f.endswith('.csv')]

    for file_name in measurement_files:

        dataset = file_name.replace('.csv', '')


        table_name = f"measurement_{dataset}"


        csv_file = os.path.join(measurements_dir, file_name)


        create_foreign_table(table_name, csv_file)

    print("Created foreign tables for measurement")



def create_table(scheme ,table_name, columns):
    cursor = conn.cursor()
    drop_query = f"DROP TABLE IF EXISTS {scheme}.{table_name.format()} CASCADE;"
    cursor.execute(drop_query)
    conn.commit()

    create_query = f"CREATE TABLE {scheme}.{table_name} ({', '.join(columns)})"
    

    cursor.execute(create_query)
    
    conn.commit()
    cursor.close()


def import_csv_to_table(schema, csv_file, table_name):
    cursor = conn.cursor()
    
    with open(csv_file, 'r') as file:

        csv_data = csv.reader(file)
        columns = next(csv_data)  
        

        insert_query = f"INSERT INTO {schema}.{table_name} VALUES ({', '.join(['%s'] * len(columns))})"
        

        cursor.execute(f"SET search_path TO {schema}")
        

        file.seek(0)  
        next(csv_data) 
        cursor.copy_from(file, table_name, sep=',', null='', columns=columns)
        
    conn.commit()
    cursor.close()


def merge_all_scheme(schema, table_name):
    cur = conn.cursor()


    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='external';")
    external_tables = cur.fetchall()


    for table in external_tables:
        cur.execute(f"INSERT INTO {schema}.{table_name} SELECT * FROM external.{table[0]};")


    conn.commit()
    cur.close()
    conn.close()



def get_csv_files():
    csv_files = []
    measurement_dir = 'data/measurement'
    for file_name in os.listdir(measurement_dir):
        if file_name.endswith('.csv'):
            csv_files.append(os.path.join(measurement_dir, file_name))
    return csv_files

def get_connection():
    print("Start DB connection")
    global conn
    attempts = 0
    max_attempts = 5
    delay = 2  
    
    while attempts < max_attempts:
        try:
            conn = psycopg2.connect(
                host=hostname,
                database=database,
                user=username,
                password=password
            )
            print("Successfully connection to DB")
            break
        except psycopg2.Error as e:
            print("Connectiong error:", e)
            attempts += 1
            print(f"Another connection attempt after {delay} second...")
            time.sleep(delay)


verify_database_info()


get_connection()

if conn is not None:
    print("Creating schema data")
    create_scheme('data')

    install_file_fdw('data')
    create_file_server('data')

    print("Creating table region")

    create_table('data', 'regions', ['identifier SERIAL PRIMARY KEY', 'description TEXT'])

    print("Creating table country")

    create_table('data', 'countries', ['identifier SERIAL PRIMARY KEY', 'region INTEGER REFERENCES data.regions(identifier)', 'description TEXT'])

    print("Creating table city")

    create_table('data', 'cities', ['identifier SERIAL PRIMARY KEY', 'country INTEGER REFERENCES data.countries(identifier)', 'description TEXT', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION', 'dataset TEXT'])

    print("Creating table measurement")

    create_table('data','measurement', ['city INTEGER REFERENCES data.cities(identifier)', 'mark TIMESTAMP WITHOUT TIME ZONE', 'temperature DOUBLE PRECISION'])

    print("Creating table coastline")

    create_table('data', 'coastline', ['shape INTEGER', 'segment INTEGER', 'latitude DOUBLE PRECISION', 'longitude DOUBLE PRECISION'])


    try:
        transfer_files_to_database()
    except:
        print(f"Cant connect to SFTP. Skip") #legacy


    create_measurement_foreign_tables()

    print("Import data in table region from data/regions.csv")

    import_csv_to_table('data', 'data/regions.csv', 'regions')

    print("Import data in table country from data/countries.csv")

    import_csv_to_table('data', 'data/countries.csv', 'countries')

    print("Import data in table city from data/cities.csv")

    import_csv_to_table('data', 'data/cities.csv', 'cities')

    print("Import data in table coastlines from data/coastline.csv")

    import_csv_to_table('data', 'data/coastline.csv', 'coastline')

    print("Joining all foreign tables from external in measurement")
    merge_all_scheme('data', 'measurement')

    conn.close()
else:
    print("Connection error. is ip-address correct?")
