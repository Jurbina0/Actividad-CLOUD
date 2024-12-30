import pymysql
import json
import datetime

# Database connection configuration
db_host = "localhost"
db_user = "root"
db_password = "admin"
db_name = "inmobiliaria"

def get_db_connection():
    """Create a connection to the MySQL database."""
    connection = pymysql.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name,
        connect_timeout=5
    )
    return connection

def get_users():
    """Function to fetch all users from the 'usuarios' table."""
    try:
        # Open a database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # SQL query to fetch users
        query = "SELECT * FROM usuarios;"
        cursor.execute(query)
        result = cursor.fetchall()

        # Convert query result to a list of dictionaries
        users = [{"id": row[0],
                  "nombre": row[1], 
                  "apellido1": row[2], 
                  "apellido2": row[3], 
                  "email": row[4], 
                  "consentimiento": str(row[5]), 
                  "fecha_creacion": row[6].strftime('%Y-%m-%d %H:%M:%S'), 
                  "fecha_modificacion": row[7].strftime('%Y-%m-%d %H:%M:%S')} for row in result]

        connection.close()

        return {
            "statusCode": 200,
            "body": json.dumps({"status": "success", "users": users})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

def add_user(event):
    """Function to add a new user to the 'usuarios' table."""
    try:
        print(event['body'])
        print(type(event['body']))
        if type(event['body']) == str: 
            data = json.loads(event['body'])
        else: 
            data = event["body"]

        # Extract user data from the POST request body
        nombre = data.get('nombre')
        apellido1 = data.get('apellido1')
        apellido2 = data.get('apellido2')
        email = data.get('email')
        consentimiento = data.get('consentimiento')

        # Open a database connection
        connection = get_db_connection()
        cursor = connection.cursor()

        # SQL query to insert a new user into the table
        query = """INSERT INTO usuarios (nombre, apellido1, apellido2, email, consentimiento)
                   VALUES (%s, %s, %s, %s, %s);"""
        cursor.execute(query, (nombre, apellido1, apellido2, email, consentimiento))
        connection.commit()
        connection.close()

        return {
            "statusCode": 201,
            "body": json.dumps({"status": "success", "message": "User added successfully."})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

def lambda_handler(event, context):
    """Main Lambda function handler."""
    if event['httpMethod'] == 'GET':
        return get_users()
    elif event['httpMethod'] == 'POST':
        print('pasa por aqui')
        return add_user(event)
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"status": "error", "message": "Method not allowed"})
        }

context = ""
# event = {"httpMethod": "POST", 
#          "body": {
#              "nombre": "MARIA",
#              "apellido1": "CORREAS",
#              "apellido2": "CRESPO",
#              "email": "test@test.com",
#              "consentimiento": True

#          }}

event = {"httpMethod": "GET"
         }

print(lambda_handler(event, context))