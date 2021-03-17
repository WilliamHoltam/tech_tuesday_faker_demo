import mysql.connector
from faker import Faker

# Set localization
fake = Faker('en_GB')

connection = mysql.connector.connect(
    user='william',
    password='password',
    host='localhost',
    database='test_db'
)

mySql_insert_query = """INSERT INTO users (name, address, ipv4_address)
                       VALUES (%s, %s, %s) """

# Define 100 records to insert with fake data.
records_to_insert = []
for i in range(100):
    records_to_insert.append(
        [fake.name(), fake.address(), fake.ipv4_private()])

cursor = connection.cursor()
cursor.executemany(mySql_insert_query, records_to_insert)
connection.commit()
print(cursor.rowcount, "records inserted successfully into users table")

cursor.close()
connection.close()

print("MySQL connection is closed")
