import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


def get_user(conn, user_id):
    """
    Fetches a user by their ID from the 'usuarios' table.
    """
    if not isinstance(user_id, int) or user_id <= 0:
        return {
            "statusCode": 400,
            "body": json.dumps({"status": "error", "message": "Invalid user_id"})
        }

    try:
        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:
                # Parameterized query to prevent SQL injection
                query = f"""
                SELECT 
                    id_usuario, 
                    nombre, 
                    apellido1, 
                    apellido2, 
                    email, 
                    consentimiento, 
                    fecha_creacion, 
                    fecha_modificacion,
                    fecha_baja 
                FROM usuarios 
                WHERE id_usuario = {user_id}
                AND fecha_baja is NULL;
                """
                cursor.execute(query)
                
                # Fetch the result
                result = cursor.fetchone()
                
                if result is None:
                    return {
                        "statusCode": 404,
                        "body": json.dumps({"status": "error", "message": "User not found"})
                    }

                # Convert result to a dictionary, handling NULL values
                user = {
                    "id": result[0],
                    "nombre": result[1],
                    "apellido1": result[2],
                    "apellido2": result[3] if result[3] is not None else None,  
                    "email": result[4],
                    "consentimiento": str(result[5]),
                    "fecha_creacion": result[6].strftime('%Y-%m-%d %H:%M:%S'),
                    "fecha_modificacion": result[7].strftime('%Y-%m-%d %H:%M:%S'), 
                    "fecha_baja": result[8].strftime('%Y-%m-%d %H:%M:%S') if result[8] else None  
                }

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "user": user})
                }

    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error fetching user with ID {user_id}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": "Internal server error"})
        }




def get_users(conn):
    """Function to fetch all users from the 'usuarios' table."""
    try:

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:

                # SQL query to fetch users
                query = """
                        SELECT 
                            id_usuario, 
                            nombre, 
                            apellido1, 
                            apellido2, 
                            email, 
                            consentimiento, 
                            fecha_creacion, 
                            fecha_modificacion,
                            fecha_baja 
                        FROM usuarios
                        WHERE fecha_baja is NULL;
                        """
                cursor.execute(query)
                query_results = cursor.fetchall()

                # Convert query result to a list of dictionaries
                users = [{"id": result[0],
                            "nombre": result[1],
                            "apellido1": result[2],
                            "apellido2": result[3] if result[3] is not None else None,  
                            "email": result[4],
                            "consentimiento": str(result[5]),
                            "fecha_creacion": result[6].strftime('%Y-%m-%d %H:%M:%S'),
                            "fecha_modificacion": result[7].strftime('%Y-%m-%d %H:%M:%S'), 
                            "fecha_baja": result[8].strftime('%Y-%m-%d %H:%M:%S') if result[8] else None } for result in query_results]

        

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "users": users})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }

def add_user(conn, body):
    """Function to add a new user to the 'usuarios' table."""
    try:

        # Extract user data from the POST request body
        nombre = body.get('nombre')
        apellido1 = body.get('apellido1')
        apellido2 = body.get('apellido2')
        email = body.get('email')
        consentimiento = body.get('consentimiento')

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:

                # SQL query to insert a new user into the table
                query = """INSERT INTO usuarios (nombre, apellido1, apellido2, email, consentimiento)
                        VALUES (%s, %s, %s, %s, %s);"""
                cursor.execute(query, (nombre, apellido1, apellido2, email, consentimiento))
                connection.commit()

                return {
                    "statusCode": 201,
                    "body": json.dumps({"status": "success", "message": "User added successfully."})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }
    

def update_user(conn, user_id, body):
    """
    Function to update user information in the database.
    """
    try:
        # Validate input fields
        required_fields = ['nombre', 'apellido1', 'apellido2', 'email', 'consentimiento']
        missing_fields = [field for field in required_fields if field not in body]

        if missing_fields:
            return {
                "statusCode": 400,
                "body": json.dumps({"status": "error", "message": f"Missing fields: {', '.join(missing_fields)}"})
            }

        # Extract user data from the request body
        nombre = body.get('nombre')
        apellido1 = body.get('apellido1')
        apellido2 = body.get('apellido2')
        email = body.get('email')
        consentimiento = body.get('consentimiento')

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:
                # SQL query to update user information
                query = """
                UPDATE usuarios
                SET nombre = %s, apellido1 = %s, apellido2 = %s, email = %s, consentimiento = %s
                WHERE id_usuario = %s;
                """
                cursor.execute(query, (nombre, apellido1, apellido2, email, consentimiento, user_id))
                connection.commit()

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "message": "User updated successfully."})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }


def delete_user(conn, user_id):
    """
    Function to update user information in the database.
    """
    try:
        
        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:
                # SQL query to update user information
                query = """
                UPDATE usuarios
                SET fecha_baja = CURRENT_TIMESTAMP
                WHERE id_usuario = %s;
                """
                cursor.execute(query, (user_id))
                connection.commit()

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "message": "User deleted successfully."})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }