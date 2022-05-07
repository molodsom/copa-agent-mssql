import requests
import pyodbc
from settings import config


def get_cursor():
    connect_params = {
        "DRIVER": "{ODBC Driver 17 for SQL Server}",
        "SERVER": config['MSSQL_HOST'],
        "DATABASE": config['MSSQL_NAME'],
        "UID": config['MSSQL_USER'],
        "PWD": config['MSSQL_PASS'],
    }
    connection = pyodbc.connect(";".join([f"{k}={v}" for k, v in connect_params.items()]))
    return connection.cursor()


def request_data():
    s = requests.Session()
    s.headers = {"Authorization": config['COPA_API_TOKEN']}
    response = s.get(config['COPA_API_URL'] + "/globalspeed/users/")
    if response.status_code != 200:
        raise ConnectionError(response.text)
    return response.json()


def main():
    data = request_data()
    inserts = [(4654, row['first_name'], row['last_name'], 50, 20, 0, 0, 0, 1, 3, 0) for row in data]
    cursor = get_cursor()
    cursor.executemany("""
        INSERT INTO [dbo].[tblUser] (
            [PIN], [Name], [Surname], [id_tblAdminSettings], [Role], [PasswordHashed],
            [HasPicture], [HasProfilePicture], [IsActiv], [LanguageID], [IsTrainer]
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""", inserts)
    cursor.commit()


if __name__ == '__main__':
    main()
