import pyodbc
from create_database import create_database


# Replace the placeholders with your Azure SQL server details
server = 'nycpayrollsqldbserver.database.windows.net'
database = 'udacity'
username = 'pysqladmin'
password = 'Bluesky1'
driver= '{ODBC Driver 18 for SQL Server}' # or use another driver version as appropriate


# Connection string
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

select_employee_master_table = "select top 5 * from NYC_Payroll_EMP_MD"
select_jobTitle_master_table = "select top 5 * from NYC_Payroll_TITLE_MD"
select_agency_master_table = "select top 5 * from NYC_Payroll_AGENCY_MD"
select_payroll2020_trans_table = "select top 5 * from NYC_Payroll_Data_2020"
select_payroll2021_trans_table = "select top 5 * from NYC_Payroll_Data_2021"



table_names = ['NYC_Payroll_EMP_MD', 'NYC_Payroll_TITLE_MD', 'NYC_Payroll_AGENCY_MD',
                'NYC_Payroll_Data_2020', 'NYC_Payroll_Data_2021']

all_select_query_table = [select_employee_master_table, select_jobTitle_master_table, select_agency_master_table,
                          select_payroll2020_trans_table, select_payroll2021_trans_table]


try:
    # Connect to your database
    connection = pyodbc.connect(connection_string)

    for i,query in enumerate(all_select_query_table):
        try:
            print(f'#------------------{table_names[i]}---------------------------#')
            #-------------------------------------------------#
            cursor = connection.cursor()
            cursor.execute(query)
            for row in cursor:
                print(row)
            
            cursor.close()
            print('#---------------------------------------------------------------#')
            #-------------------------------------------------#
        except Exception as e:
            print("An error occurred:", e)
            print("Failed to create table.")
            cursor.close()
except Exception as e:
    print("An error occurred:", e)
    print("Failed connection to database.")
finally:
    # Clean up
    connection.close()
