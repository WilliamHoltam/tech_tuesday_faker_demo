import mysql.connector

# Configure connection to MySQL database
connection = mysql.connector.connect(
    user='william',
    password='password',
    host='localhost',
    database='test_db'
)

# Define query
mySql_insert_query = """INSERT INTO users (name, address, ipv4_address)
                       VALUES (%s, %s, %s) """

print(cursor.rowcount, "records inserted successfully into users table.")

HELLO THIS IS WRONG CODE

cursor.close()
connection.close()

print("MySQL connection is closed")
