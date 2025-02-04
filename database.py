import mysql.connector

def get_db_connection():
    return mysql.connector.connect(
        host="Sahilgholap007.mysql.pythonanywhere-services.com",
        user="Sahilgholap007",  # Your PythonAnywhere MySQL username
        password="Lihas@007",    # Your MySQL password
        database="Sahilgholap007$marketplace_tracker"  # Your full database name
    )
