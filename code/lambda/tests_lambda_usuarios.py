import lambda_usuarios as l

context = ""
# event = {"httpMethod": "POST", 
#          "path": "users", 
#          "headers": "",
#          "body": {
#              "nombre": "MARIA",
#              "apellido1": "CORREAS",
#              "apellido2": "CRESPO",
#              "email": "test@test.com",
#              "consentimiento": True

#          }}

# event = {"httpMethod": "GET",
#          "path": "users", 
#          "headers": "",
#          "body":{}
#          }

event = {"httpMethod": "GET",
         "path": "user", 
         "pathParameters": {"user_id": 1},
         "headers": "",
         "body":{}
         }

# event = {"httpMethod": "UPDATE", 
#          "path": "user", 
#          "headers": "",
#          "pathParameters": {"user_id": 1},
#          "body": {
#              "nombre": "MARIA",
#              "apellido1": "CORREAS",
#              "apellido2": "CRESPO",
#              "email": "test2@test.com",
#              "consentimiento": True

#          }}


# event = {"httpMethod": "DELETE",
#          "path": "user", 
#          "pathParameters": {"user_id": 1},
#          "headers": "",
#          "body":{}
#          }
print(l.lambda_handler(event, context))