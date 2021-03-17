"""Bulk insert fake 'Faker' data into MySQL tables."""

import logging
import random
from pathlib import Path

import mysql.connector
from faker import Faker

log_filename = Path().cwd() / 'faker_insert.log'
logging.basicConfig(filename=log_filename, level=logging.DEBUG)

connection_parameters = {
    'user': 'william',
    'password': 'password',
    'host': '127.0.0.1',
    'database': 'test_db',
}


class MySQL:
    """Class handles connectivity and communication with MySQL Server"""

    def __init__(self, connection_parameters):
        """Initialise MySQL attributes"""
        self.connection_parameters = connection_parameters
        self.connection = None

    def open_connection(self):
        """Configure connection to MySQL database"""
        try:
            self.connection = mysql.connector.connect(
                **self.connection_parameters)
        except mysql.connector.Error as error:
            logging.error(
                'MySQL query executed on MySQL TestDB failed: ' + error)
            if self.connection.is_connected():
                self.connection.close()
            raise
        else:
            logging.info('MySQL query executed on MySQL TestDB successful.')

    def create_table(self, query):
        """Run create table query on a DB connection"""
        table_name = query.split()[2]
        try:
            cursor = self.connection.cursor()
            cursor.execute(operation=query)
            self.connection.commit()
        except mysql.connector.DataError as error:
            error_string = " ".join(
                ['Creation of', table_name, 'table is unsuccessful:', error])
            logging.error(error_string)
            logging.debug('This query failed: ', query)
            raise
        else:
            logging.info(table_name + ' created successfully.')
        finally:
            cursor.close()

    def bulk_insert(self, query, input_data):
        """Perform a bulk insert over a DB connection"""
        table_name = query.split()[2]
        try:
            cursor = self.connection.cursor()
            cursor.executemany(operation=query, seq_params=input_data)
            self.connection.commit()
        except mysql.connector.DataError as error:
            logging.error('Query is unsucccessful: ' + error)
            logging.debug('This query failed: ' + query)
            raise
        else:
            logging.info(" ".join([
                str(cursor.rowcount),
                'records inserted successfully into',
                table_name,
                'table',
            ]))
        finally:
            cursor.close()

    def close_connection(self):
        try:
            if self.connection.is_connected():
                self.connection.close()
        except mysql.connector.Error as error:
            logging.error(
                'MySQL connector unable to close connection:' + error)
            raise
        else:
            logging.info('MySQL connector connection closed successfully.')


def generate_n_customers(n_rows: int):
    """Generate x rows of customer data"""
    records_to_insert = []
    for i in range(n_rows):
        customers_data_one_row = (
            fake.first_name(),
            fake.last_name(),
            fake.address(),
            fake.phone_number(),
        )
        records_to_insert.append(customers_data_one_row)
    logging.info(" ".join(['Generated', str(n_rows), 'rows of data.']))
    return records_to_insert


def generate_n_credit_cards(n_rows: int, customer_id_range: tuple):
    """Generate x rows of credit_card data"""
    records_to_insert = []
    for i in range(n_rows):
        credit_card_data_one_row = (
            fake.credit_card_provider(),
            fake.credit_card_number(),
            fake.credit_card_security_code(),
            fake.credit_card_expire(),
            random.randint(customer_id_range[0], customer_id_range[1]),
        )
        records_to_insert.append(credit_card_data_one_row)
    logging.info(" ".join(['Generated', str(n_rows), 'rows of data.']))
    return records_to_insert


def read_mysql_file(filepath):
    """Read a mysql_file and return a list of commands"""
    with open(filepath, 'r') as mysql_ddl_file:
        mysql_ddl = mysql_ddl_file.read().split(';')
    return mysql_ddl


if __name__ == '__main__':
    fake = Faker('en_GB')
    mysql_instance = MySQL(connection_parameters=connection_parameters)
    mysql_instance.open_connection()
    mysql_ddl = read_mysql_file(filepath='./ddl.sql')
    mysql_instance.create_table(query=mysql_ddl[0])
    customers_insert_query = """INSERT INTO customers (first_name, last_name, address, phone_number)
                                VALUES (%s, %s, %s, %s)"""
    num_customers = 100
    customers_data_all_rows = generate_n_customers(n_rows=num_customers)
    mysql_instance.bulk_insert(
        query=customers_insert_query,
        input_data=customers_data_all_rows,
    )
    mysql_instance.create_table(query=mysql_ddl[1])
    credit_cards_insert_query = """
    INSERT INTO credit_cards (
        provider,
        number,
        security_code,
        expiry_date,
        customer_id)
    VALUES (%s, %s, %s, %s, %s)"""
    num_credit_cards = 20000
    credit_card_data_all_rows = generate_n_credit_cards(
        n_rows=num_credit_cards,
        customer_id_range=(1, num_customers),
    )
    mysql_instance.bulk_insert(
        query=credit_cards_insert_query,
        input_data=credit_card_data_all_rows,
    )
    mysql_instance.close_connection()
