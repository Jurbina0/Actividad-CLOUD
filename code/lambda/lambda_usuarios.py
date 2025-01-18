import pymysql
import json
import logging
import os
import users_functions as u
import favorites_functions as f  
import properties_functions as p

# Configure logging
logging.basicConfig(level=logging.INFO)

# Database connection configuration from environment variables
db_host = os.getenv("DB_HOST")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")



def get_db_connection():
    """Create a connection to the MySQL database."""
    try:
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            connect_timeout=5
        )
        return connection
    except Exception as e:
        logging.error(f"Error connecting to the database: {e}")
        raise

def lambda_handler(event, context):
    """Main Lambda function handler."""

    logging.info(event)
    path = event['path']
    method = event['httpMethod']
    headers = event['headers']
    query_params = event.get('queryStringParameters')
    path_params = event.get('pathParameters')

    conn = None
    try:
        conn = get_db_connection()
        
        
        body = json.loads(event['body']) if event.get('body') else {}

        if path == '/all_users' and method == 'GET':
            result = u.get_users(conn)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/all_users' and method == 'POST':
            result = u.add_user(conn, body)
            return {"statusCode": 201, "body": json.dumps(result)}
        
        elif path == '/user' and method == 'GET':
            user_id = path_params.get('user_id')
            result = u.get_user(conn, user_id)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/user' and method == 'PUT':
            user_id = path_params.get('user_id') if path_params else None
            result = u.update_user(conn, user_id, body)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/user' and method == 'DELETE':
            user_id = path_params.get('user_id') if path_params else None
            result = u.delete_user(conn, user_id)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/favorite_properties' and method == 'GET':
            user_id = path_params.get('user_id') if path_params else None
            result = f.get_favorites(conn, user_id)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/favorite_properties' and method == 'POST':
            user_id = path_params.get('user_id') if path_params else None
            result = f.add_favorite(conn, user_id, body)
            return {"statusCode": 201, "body": json.dumps(result)}

        elif path == '/favorite_properties' and method == 'PUT':
            user_id = path_params.get('user_id') if path_params else None
            property_id = path_params.get('property_id') if path_params else None
            result = f.remove_favorite(conn, user_id, property_id)
            return {"statusCode": 201, "body": json.dumps(result)}
        
        elif path == '/all_properties' and method == 'GET':
            result = p.get_all_properties(conn)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        elif path == '/properties' and method == 'GET':
            result = p.get_properties_query_params(conn, body)
            return {"statusCode": 200, "body": json.dumps(result)}
        
        else:
            return {"statusCode": 405, "body": json.dumps({"status": "error", "message": "Method not allowed"})}
    except Exception as e:
        logging.error(f"Error: {e}")
        return {"statusCode": 500, "body": json.dumps({"status": "error", "message": str(e)})}

