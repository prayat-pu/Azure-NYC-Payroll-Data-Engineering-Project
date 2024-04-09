import pyodbc
from create_database import create_database


# Replace the placeholders with your Azure SQL server details
server = 'nycpayrollsqldbserver.database.windows.net'
database = 'udacity'
username = 'pysqladmin'
password = 'Bluesky1'
driver= '{ODBC Driver 18 for SQL Server}' # or use another driver version as appropriate

create_database(server,username,password,database)

# Connection string
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};UID={username};PWD={password}'

create_employee_master_table = """
CREATE TABLE NYC_Payroll_EMP_MD (
    EmployeeID varchar(10) NULL,
    LastName varchar(20) NULL,
    FirstName varchar(20) NULL
)
"""

create_jobTitle_master_table = """
CREATE TABLE NYC_Payroll_TITLE_MD (
    TitleCode varchar(10) NULL,
    TitleDescription varchar(100) NULL
)
"""

create_agency_master_table = """
CREATE TABLE NYC_Payroll_AGENCY_MD (
    AgencyID varchar(10) NULL,
    AgencyName varchar(50) NULL
)
"""

create_payroll2020_trans_table = """
CREATE TABLE NYC_Payroll_Data_2020 (
    FiscalYear int NULL,
    PayrollNumber int NULL,
    AgencyID varchar(10) NULL,
    AgencyName varchar(50) NULL,
    EmployeeID varchar(10) NULL,
    LastName varchar(20) NULL,
    FirstName varchar(20) NULL,
    AgencyStartDate date NULL,
    WorkLocationBorough varchar(50) NULL,
    TitleCode varchar(10) NULL,
    TitleDescription varchar(100) NULL,
    LeaveStatusasofJune30 varchar(50) NULL,
    BaseSalary float NULL,
    PayBasis varchar(50) NULL,
    RegularHours float NULL,
    RegularGrossPaid float NULL,
    OTHours float NULL,
    TotalOTPaid float NULL,
    TotalOtherPay float NULL
)
"""

create_payroll2021_trans_table = """
CREATE TABLE NYC_Payroll_Data_2021 (
    FiscalYear int NULL,
    PayrollNumber int NULL,
    AgencyID varchar(10) NULL,
    AgencyName varchar(50) NULL,
    EmployeeID varchar(10) NULL,
    LastName varchar(20) NULL,
    FirstName varchar(20) NULL,
    AgencyStartDate date NULL,
    WorkLocationBorough varchar(50) NULL,
    TitleCode varchar(10) NULL,
    TitleDescription varchar(100) NULL,
    LeaveStatusasofJune30 varchar(50) NULL,
    BaseSalary float NULL,
    PayBasis varchar(50) NULL,
    RegularHours float NULL,
    RegularGrossPaid float NULL,
    OTHours float NULL,
    TotalOTPaid float NULL,
    TotalOtherPay float NULL
)
"""

create_payroll2021_trans_table = """
    CREATE TABLE NYC_Payroll_Summary (
    FiscalYear int NULL,
    AgencyName varchar(50) NULL,
    TotalPaid float NULL
)
"""

table_names = ['NYC_Payroll_EMP_MD', 'NYC_Payroll_TITLE_MD', 'NYC_Payroll_AGENCY_MD',
                'NYC_Payroll_Data_2020', 'NYC_Payroll_Data_2021','NYC_Payroll_Summary']
all_create_query_table = [create_employee_master_table, create_jobTitle_master_table, create_agency_master_table,
                          create_payroll2020_trans_table, create_payroll2021_trans_table,create_payroll2021_trans_table]


try:
    # Connect to your database
    connection = pyodbc.connect(connection_string)

    for i,create_tb_query in enumerate(all_create_query_table):
        try:
            #-------------------------------------------------#
            ## delete table if exist
            cursor = connection.cursor()
            # Create a new table named Employees
            cursor.execute(f'drop table if exists {table_names[i]}')
            # Commit the transaction
            connection.commit()
            print("drop table successfully.")
            #-------------------------------------------------#


            #-------------------------------------------------#
            # Create a new table named Employees
            cursor.execute(create_tb_query)
            # Commit the transaction
            connection.commit()
            print("Table created successfully.")
            cursor.close()
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
