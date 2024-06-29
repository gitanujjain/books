import sys
import pymssql
from awsglue.utils import getResolvedOptions

import psycopg2

class PostgresConectionManger:
    """
    this class for postgres database.
    """

    def __init__(self, connection_info) -> None:
        """
        initalize value connection details and create the connection with oracle database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '5432')
        self.database_name = connection_info.get('database_name', '')
        self.schema_name = connection_info.get('schema_name', '')
        self.password = connection_info.get('password', '')
        self.source_schema = connection_info.get('source_schema', None)
        err, self.connection = self.create_connection()
        if self.connection is None:
            raise Exception(err)

    db_params = {
        "host": "your_database_host",
        "database": "your_database_name",
        "user": "your_database_user",
        "password": "your_database_password",
        "port": "your_database_port",  # Default is 5432 for PostgreSQL
    }

    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                    "host": self.host_address,
                    "database": self.database_name,
                    "user": self.schema_name,
                    "password": self.password,
                    "port": self.port_number,  # Default is 5432 for PostgreSQL
                }
            con = psycopg2.connect(**params)
        except Exception as err:
            return err, con
        return err, con

    def insert_data(self,table_name,data):
        result = None
        sql_query=f'insert into {table_name} values {data}'
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            self.connection.commit()

    def connection_close(self):
        self.connection.close()


class MsSqlConectionManger:
    """
    this class for MsSQL the connection and detail of databse.
    """

    def __init__(self, connection_info) -> None:
        """
        initalize value connection details and create the connection with mssql database
        """
        self.host_address = connection_info.get('host_address', '')
        self.port_number = connection_info.get('port_number', '1433')
        self.database_name = connection_info.get('database_name', '')
        self.user_name = connection_info.get('username', '')
        self.password = connection_info.get('password', '')
        err, self.connection = self.create_connection()
        if self.connection is None:
            print("Connection not created ")
            raise Exception(err)

    def create_connection(self):
        """ Connect to the oracle Server database server """
        con = None
        err = None
        try:
            params = {
                'server': self.host_address,
                'user': self.user_name,
                'password': self.password,
                'database': self.database_name,
                'port': self.port_number
            }
            print(params)
            con = pymssql.connect(**params)
            print("con-----------", con)
        except Exception as err:
            print(err)
            return err, con
        return err, con

    def connection_close(self):
        self.connection.close()


    def call_store_procedure(self,prams):
        result=None
		sql_query=f"CALL SF_TEST_DB.STRUCTURED.TEST() {prams};"
        print(sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            data = cursor.fetchone()
            result = data[0]
        return result

    def select_data(self,table_name,prams):
        result=None
		sql_query=f"select * from {table_name} where id = {prams}"
        print(sql_query)
        with self.connection.cursor() as cursor:
            cursor.execute(sql_query)
            data = cursor.fetchall()
        return result


args = getResolvedOptions(sys.argv, [‘host_address’, ‘port_number’, ‘database_name’, ‘username’, ‘password’, 'parms'])
ms_conn_info={
	"host_address" :args['host_address'],
	"port_number" :args['port_number'],
	"database_name" :args['database_name'],
	"user_name" : args['username'],
	"password" : args['password']
}
prams=args['prams']
table_name=''
post_conn_info={
	"host_address" :args['host_address'],
	"port_number" :args['port_number'],
	"database_name" :args['database_name'],
	"user_name" : args['username'],
	"password" : args['password']
}
try :
	mssql= MsSqlConectionManger(conn_info)
	postgres=PostgresConectionManger(conn_info)
    data=mssql.select_data(table_name,prams)
	for each in data:
        postgres.insert_data(table_name,each)
    mssql.connection_close()
except Exception as err:
	print(err)
