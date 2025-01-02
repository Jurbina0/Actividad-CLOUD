# Actividad-CLOUD
Repositorio privado para el desarrollo y ejecución de la actividad Actividad CLOUD asociada a la asignatura Análisis de datos en entornos Big Data. Miembros: María Correas Crespo y Judith Urbina.

Enlace al overleaf:
https://www.overleaf.com/7431459492vshwjsngbdmn#16858d

---
Indicaciones!!

Ir colgando al github 

Judith:

Paso 1: crear proceso ETL, leer datos fichero CSV, procesarlos (limpiar, quitar nulos, duplicados, formato que queremos, columna como metros cuadrados) y guardar datos limpios en otro fichero

Paso 2: añadir al proceso ETL guardar datos limpios en una base de datos (crear base de datos destino, conectarse a una base de datos, meter los datos)
SSMS necesario y esperar instrucciones de María

(
id de la vivienda índice
fecha creación today
fecha modificación null
fecha baja null
)

tabla 1: viviendas
ETL paso 2
tabla 2: históricos precios

----------------------
María:

- API Usuarios
- Creación Base de Datos de Destino

------------------------------------------

Reglas escuchando a distintos servicios: automatizamando: entra CSV avisa al ETL de GLUE y empieza a trabajar
You
12:23 PM
Cloudwatch registra logs _  localiza error y notifica a la persona de mantenimiento enviando un correo - se facilita el registro de errores

----------------------------------
inmobiliaria servicio de compraventa 
-----------------------------------
Creamos
- Servicio RDS: base de datos
- Servicio AWS Glue: ETL (sólo para el csv)
- Servicio lambda:
    - Lambda 1: API Viviendas
    - Lambda 2: API Usuarios
- 
  
KPI:
-  API: servicio de consulta funciona correctamente, se realiza un correcto registro de los usuarios
  
Dos fuentes de datos
- API de clientes: lambda que interactúa con los cliente.
(post, get, update, delete)

post registro_usuario: registrar los usuarios
get info_usuario: obtener info de los usuarios
update actualizar_tabla: actualizar la info
delete dar_de_baja: para dar de baja a un usuario previamente registro
- API de consulta de viviendas: lambda que responde a las consultas de los usuarios
get viviendas: obtener viviendas según filtros.
post marcar_favorito: marcar favorito. con el identificador del usuario y de la vivienda establecemos la relación de favorito que crea el registro en la base de datos.
get viviendas_favoritas: obtener viviendas según el filtro que indica la relación creada gracias a marcar_favorito.
- csv en un bucket: procesamos con ETL script de python (limpiamos y creamos nuevas columnas si conviene) y las ingestamos en la base de datos

-----

postman entorno para hacer pruebas
librería request en lambda
archivo Swagger: documentación del API. 
  Interactivo: https://editor.swagger.io/ 
Proceso batch y no batch

---

**Amazon S3 Bucket**

- nombre: glue-inmobiliaria-bucket-1
- availability zone: N.virginia us-east-1
- enable bucket versioning

Estructura del folder

data_landing_zone - load-20250102

**Amazon Glue**

Connection to MySQL ya que María usa MySQL Workbench

**Amazon IAM**

nuevo rol con la configuración que sigue
- name: glue-inmobiliaria-role
- use case: Glue
- allowed policies: AmazonS3FullAccess, CloudWatchLogsFullAccess, GlueConsoleFullAccess

Error a la hora de crear el rol:
Analysis
The error is caused by insufficient permissions configured for the principal arn:aws:iam::258567897508:role/voclabs.
Resolution
Try the following steps to resolve your error:


Navigate to the IAM console, and then navigate to the voclabs role.
Choose the Permissions tab, and then choose Add permissions.
Choose the Create inline policy option.
Navigate to the JSON tab in the policy editor.
Copy and paste the following policy document into the policy editor. This policy document is a starting point, and you might want to add more permissions. Edit the "Action" and "Resource" fields to match the specific actions and resources required for your use case:
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:CreateRole"
            ],
            "Resource": [
                "arn:aws:iam::258567897508:role/glue-inmobiliaria-role"
            ]
        }
    ]
}
Review the updated policy for correctness, and then choose Next.
Input the policy name, review the permissions defined in the policy, and then choose Create Policy to create the inline policy.
By following these steps, you can grant the necessary permissions to the voclabs role to perform iam:CreateRole on the resource arn:aws:iam::258567897508:role/glue-inmobiliaria-role.

Note: If the action doesn't support resource level permissions, you can use star * in the resource field.