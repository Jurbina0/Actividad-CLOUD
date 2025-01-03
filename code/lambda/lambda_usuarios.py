import pymysql
import json
import datetime
import logging
import users_functions as u
import favorites_functions as f
# Configure logging
logging.basicConfig(level=logging.INFO)


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


def lambda_handler(event, context):
    """Main Lambda function handler."""

    path = event['path']                
    method = event['httpMethod']       
    headers = event['headers']          
    query_params = event.get('queryStringParameters')  
    path_params = event.get('pathParameters')  

    conn = get_db_connection()

    if event['body']:
        if type(event['body']) == str: 
            body = json.loads(event['body'])
        else: 
            body = event["body"]
    else:
        body = {}

    if path ==  'users' and method == 'GET':
        # Método que devuelve los usuarios
        return u.get_users(conn)
    
    elif path ==  'users' and method == 'POST':
        # Método que crea usuario
        return u.add_user(conn, body)
    
    elif path ==  'user' and method == 'GET':
        # Método que  devuelve información de usuario en concreto
        user_id = path_params.get('user_id')
        return u.get_user(conn, user_id)
    
    elif path ==  'user' and method == 'UPDATE':
        # Método que  actualiza información de usuario en concreto
        user_id = path_params.get('user_id')
        return u.update_user(conn, user_id, body)
    
    elif path ==  'user' and method == 'DELETE':
        # Método que  actualiza información de usuario en concreto
        user_id = path_params.get('user_id')
        return u.delete_user(conn, user_id)

    
    elif path ==  'favorite_properties' and method == 'GET':
        user_id = path_params.get('user_id')
        # Método que devuelve las propiedades favoritas de un usuario
        return f.get_favorites(conn, user_id)
    
    elif path ==  'favorite_properties' and method == 'POST':
        user_id = path_params.get('user_id')
        # Método que crea preferencia de un usuario por una vivienda 
        return f.add_favorite(conn, user_id, body)
    
    
    else:
        return {
            "statusCode": 405,
            "body": json.dumps({"status": "error", "message": "Method not allowed"})
        }