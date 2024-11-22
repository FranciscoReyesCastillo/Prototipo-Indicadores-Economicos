import mysql.connector

def get_connection():
    config = {
        'user': 'python_user',
        'password': '',
        'host': 'localhost',
        'port': '3306',
        'database':'indicadores_economicos',
        'raise_on_warnings': True
    }

    try:
        connection = mysql.connector.connect(**config)
        return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None