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




def remove_favorite(conn, user_id, body):
    """Function to add a new favorite property to the 'viviendas_favoritas' table."""
    try:

        # Extract user data from the POST request body
        id_vivienda = body.get('id_vivienda')

        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:

                # SQL query to insert a new user into the table
                query = f"""UPDATE viviendas_favoritas 
                            SET fecha_baja = CURDATE() 
                            WHERE id_usuario = {user_id}
                            AND id_vivienda = {id_vivienda};"""
                cursor.execute(query)
                connection.commit()
                connection.close()

                return {
                    "statusCode": 201,
                    "body": json.dumps({"status": "success", "message": "Favorite property removed successfully."})
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
                    direccion
                    precio_pounds
                FROM viviendas_favoritas 
                INNER JOIN viviendas ON id_vivienda
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
                favorite_buildings = []
                for r in result:
                    favorite_building = {
                        "id_vivienda": r[0],
                        "direccion": r[1],
                        "precio_pounds": r[2]
                        }
                    
                    favorite_buildings.append(favorite_building)

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "favorite_buildings": favorite_buildings})
                }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": str(e)})
        }