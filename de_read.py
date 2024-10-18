import psycopg2


# Функция для подключения к базе данных
def connect_to_db():
    try:
        connection = psycopg2.connect(
            user="myuser",  # Имя пользователя PostgreSQL
            password="00000",  # Пароль пользователя
            host="localhost",  # Хост (если PostgreSQL работает в Docker — это localhost)
            port="54320",  # Порт, на котором работает PostgreSQL
            database="mydb"  # Название базы данных
        )
        return connection
    except Exception as error:
        print(f"Ошибка подключения к PostgreSQL: {error}")
        return None


# Функция для выполнения SQL-запроса и получения данных
def read_data_from_postgres(connection):
    try:
        cursor = connection.cursor()

        # Выполнение SQL-запроса на выборку данных
        cursor.execute("SELECT * FROM my_table;")

        # Получение всех строк результата
        rows = cursor.fetchall()

        # Печать заголовков колонок (имена колонок)
        colnames = [desc[0] for desc in cursor.description]
        print(f"Заголовки колонок: {colnames}")

        # Печать всех строк
        for row in rows:
            print(row)

        cursor.close()  # Закрытие курсора
    except Exception as error:
        print(f"Ошибка при выполнении запроса: {error}")


# Главная функция
def main():
    # Подключаемся к базе данных
    connection = connect_to_db()

    if connection is not None:
        # Читаем данные из PostgreSQL
        read_data_from_postgres(connection)

        # Закрываем соединение с базой данных
        connection.close()


if __name__ == "__main__":
    main()
