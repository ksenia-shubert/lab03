#SHUBERT_KSENIA_305

import psycopg2
import pandas as pd
import matplotlib.pyplot as plt


hostname = '192.168.122.133'
username = 'postgres'
password = 'P@ssw0rd'
database = 'lab_03'

conn = None

def drawMap():

    cursor = conn.cursor()


    query = "SELECT longitude, latitude FROM data.coastline"
    cursor.execute(query)


    results = cursor.fetchall()


    cursor.close()
    conn.close()


    data = pd.DataFrame(results, columns=['longitude', 'latitude'])


    plt.figure(figsize=(10, 6))
    plt.scatter(data['longitude'], data['latitude'], s=0.5, alpha=0.5)
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Map')


    plt.xlim(-180, 180)
    plt.ylim(-90, 90)


    plt.show()

def get_connection():
    print("Start db connection")
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
            print("Successfully connect to DB")
            break
        except psycopg2.Error as e:
            print("Connection error:", e)
            attempts += 1
            print(f"Another connection attempt after {delay} second...")
            time.sleep(delay)


def verify_database_info():
    global hostname, username, password, database, hostname_sftp, username_sftp, password_sftp
    
    print("Current connection info:")
    print(f"host: {hostname}")
    print(f"user: {username}")
    print(f"password: {password}")
    print(f"database: {database}")
    
    choice = input("Is that correct connectiong info? (1 - yes, 2 - no): ")
    
    if choice == "2":
        hostname = input("host: ")
        username = input("user: ")
        password = input("password: ")
        database = input("database: ")

verify_database_info()
get_connection()
drawMap()
