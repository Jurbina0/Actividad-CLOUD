import json
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)


def get_property(conn, property_id):
    """
    Fetches a user by their ID from the 'usuarios' table.
    """
    if not isinstance(property_id, int) or property_id <= 0:
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
                    id_vivienda, 
                    tipo_vivienda, 
                    estado_vivienda, 
                    direccion, 
                    vecindario, 
                    n_dormitorios, 
                    n_banos, 
                    tamano,
                    hay_jardin,
                    hay_garaje,
                    n_plantas,
                    tipo_calefaccion,
                    tipo_hay_terraza,
                    tipo_vistas,
                    tipo_decorado,
                    tipo_materiales,
                    ano_construccion,
                    precio_pounds,
                    precio_metro_cuadrado

                FROM viviendas 
                WHERE id_vivienda = {property_id}
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
                property_info = {
                    "id_vivienda": result[0],
                    "tipo_vivienda": result[1],
                    "direccion": result[2],
                    "vecindario": result[3],  
                    "n_dormitorios": result[4],
                    "n_banos": str(result[5]),
                    "tamano": result[6],
                    "hay_jardin": result[7], 
                    "hay_garaje": result[8],
                    "n_plantas": result[9],
                    "tipo_calefaccion":result[10],
                    "tipo_hay_terraza":result[11],
                    "tipo_vistas": result[12],
                    "tipo_decorado": result[13],
                    "tipo_materiales":result[14],
                    "ano_cosntruccion":result[15],
                    "precio_pounds":result[16],
                    "precio_metro_cuadrado":result[17]
                }

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "property": property_info})
                }

    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error fetching property with ID {property_id}: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": "Internal server error"})
        }




def get_all_properties(conn):
    """
    Fetches all available properties
    """

    try:
        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:
                # Parameterized query to prevent SQL injection
                query = f"""
                SELECT 
                    id_vivienda, 
                    tipo_vivienda, 
                    estado_vivienda, 
                    direccion, 
                    vecindario, 
                    n_dormitorios, 
                    n_banos, 
                    tamano,
                    hay_jardin,
                    hay_garaje,
                    n_plantas,
                    tipo_calefaccion,
                    tipo_hay_terraza,
                    tipo_vistas,
                    tipo_decorado,
                    tipo_materiales,
                    ano_construccion,
                    precio_pounds,
                    precio_metro_cuadrado

                FROM viviendas 
                WHERE fecha_baja is NULL;
                """
                cursor.execute(query)
                
                # Fetch the result
                results = cursor.fetchone()
                
                if results is None:
                    return {
                        "statusCode": 404,
                        "body": json.dumps({"status": "error", "message": "No properties available"})
                    }
                
                all_properties = []
                for result in results: 
                    # Convert result to a dictionary, handling NULL values
                    property_info = {
                        "id_vivienda": result[0],
                        "tipo_vivienda": result[1],
                        "direccion": result[2],
                        "vecindario": result[3],  
                        "n_dormitorios": result[4],
                        "n_banos": str(result[5]),
                        "tamano": result[6],
                        "hay_jardin": result[7], 
                        "hay_garaje": result[8],
                        "n_plantas": result[9],
                        "tipo_calefaccion":result[10],
                        "tipo_hay_terraza":result[11],
                        "tipo_vistas": result[12],
                        "tipo_decorado": result[13],
                        "tipo_materiales":result[14],
                        "ano_cosntruccion":result[15],
                        "precio_pounds":result[16],
                        "precio_metro_cuadrado":result[17]
                    }

                    all_properties.append(property_info)

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "buildings": all_properties})
                }

    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error fetching properties: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": "Internal server error"})
        }


def get_properties_query_params(conn, body):
    """
    Fetches all available properties
    """
    filters = body.get('filters')
    quantitative_filters = filters.get('quantitative_filters')
    qualitative_filters = filters.get('qualitative_filters')

    quantitative_keys = quantitative_filters.keys()
    qualitative_keys = qualitative_filters.keys()
    order_by = body.get('order_by')
                         


    try:
        # Open a database connection
        with conn as connection:
            with connection.cursor() as cursor:
                # Parameterized query to prevent SQL injection

                if order_by is not None:
                    orderby_query = f"ORDER BY {order_by};"
                else:
                    orderby_query = ";"
                
                query_filters = ""
                for q1 in qualitative_keys:
                    
                    q_list = str(qualitative_filters[q1]).replace('"', "'").replace('[', '').replace(']', '')
                    q1_query = f"AND {q1} in ({q_list})"
                    query_filters += q1_query
                
                for q2 in quantitative_keys:

                    less_than = q2['less_or_equal']
                    more_than = q2['more_or_equal']

                    if less_than is None: 
                        q2_query = f"AND {q2} >= {more_than}"
                    elif more_than is None:
                        q2_query = f"AND {q2} <= {less_than}"
                    else:
                        q2_query = f"AND {q2} <= {less_than} AND {q2} >= {more_than}"
                    
                    query_filters += q2_query

                    
                query = f"""
                SELECT 
                    id_vivienda, 
                    tipo_vivienda, 
                    estado_vivienda, 
                    direccion, 
                    vecindario, 
                    n_dormitorios, 
                    n_banos, 
                    tamano,
                    hay_jardin,
                    hay_garaje,
                    n_plantas,
                    tipo_calefaccion,
                    tipo_hay_terraza,
                    tipo_vistas,
                    tipo_decorado,
                    tipo_materiales,
                    ano_construccion,
                    precio_pounds,
                    precio_metro_cuadrado

                FROM viviendas 
                WHERE fecha_baja is NULL
                ;
                """
                query += query_filters + orderby_query

                cursor.execute(query)
                
                # Fetch the result
                results = cursor.fetchone()
                
                if results is None:
                    return {
                        "statusCode": 404,
                        "body": json.dumps({"status": "error", "message": "No properties available"})
                    }
                
                all_properties = []
                for result in results: 
                    # Convert result to a dictionary, handling NULL values
                    property_info = {
                        "id_vivienda": result[0],
                        "tipo_vivienda": result[1],
                        "direccion": result[2],
                        "vecindario": result[3],  
                        "n_dormitorios": result[4],
                        "n_banos": str(result[5]),
                        "tamano": result[6],
                        "hay_jardin": result[7], 
                        "hay_garaje": result[8],
                        "n_plantas": result[9],
                        "tipo_calefaccion":result[10],
                        "tipo_hay_terraza":result[11],
                        "tipo_vistas": result[12],
                        "tipo_decorado": result[13],
                        "tipo_materiales":result[14],
                        "ano_cosntruccion":result[15],
                        "precio_pounds":result[16],
                        "precio_metro_cuadrado":result[17]
                    }

                    all_properties.append(property_info)

                return {
                    "statusCode": 200,
                    "body": json.dumps({"status": "success", "properties": all_properties})
                }

    except Exception as e:
        # Log the error for debugging
        logging.error(f"Error fetching properties: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"status": "error", "message": "Internal server error"})
        }

    



