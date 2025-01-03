import json
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)

    

def add_favorite(conn, user_id, body):
    """Function to add a new favorite property to the 'viviendas_favoritas' table."""
    try:

        # Extract user data from the POST request body
        id_vivienda = body.get('id_vivienda')

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:

                # SQL query to insert a new user into the table
                query = """INSERT INTO viviendas_favoritas (id_usuario, id_vivienda)
                        VALUES (%s, %s);"""
                cursor.execute(query, (user_id, id_vivienda))
                connection.commit()
                connection.close()

                return {
                    "statusCode": 201,
                    "body": json.dumps({"status": "success", "message": "Favorite property added successfully."})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }


def get_favorites(conn, user_id):
    """Function to add a new favorite property to the 'viviendas_favoritas' table."""
    try:

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:

                # SQL query to insert a new user into the table
                # Parameterized query to prevent SQL injection
                query = f"""
                SELECT 
                    id_vivienda
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
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }