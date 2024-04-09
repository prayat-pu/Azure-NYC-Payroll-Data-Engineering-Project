import pyodbc

def create_database(server,username,password,new_database):

    # Connection parameters
    server = server # Update with your server name
    database = 'master' # Connects to the master database
    username = username
    password = password
    driver= '{ODBC Driver 18 for SQL Server}'

    # New database name
    new_database_name = new_database # Specify your new database name here


    connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

    try:
        # Connect and set autocommit to True
        with pyodbc.connect(connection_string, autocommit=True) as conn:
            with conn.cursor() as cursor:
                # Now safe to execute CREATE DATABASE
                cursor.execute(f"CREATE DATABASE {new_database_name}")
                print(f"Database '{new_database_name}' created successfully.")
    except Exception as e:
        print(f"An error occurred: {e}")



