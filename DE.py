import requests
import csv
import io
import psycopg2
import psycopg2.extras


# Функция для загрузки файла
def download_file(url):
    response = requests.get(url)
    if response.status_code == 200:
        content = response.content.decode('utf-8')
        return content
    else:
        print(f"Не удалось загрузить файл. Статус код: {response.status_code}")
        return None


# Функция для обработки CSV данных
def process_csv_data(csv_content):
    data = []
    reader = csv.reader(io.StringIO(csv_content))
    headers = next(reader)  # Читаем заголовки (если они есть)
    for row in reader:
        data.append(row)
    return headers, data


# Функция для корректировки заголовков столбцов
def sanitize_column_names(headers):
    # Замена недопустимых символов (например, /) на подчеркивание
    return [col.replace('/', '_').replace(' ', '_') for col in headers]


# Подключение к базе данных PostgreSQL
def connect_to_db():
    try:
        connection = psycopg2.connect(
            user="myuser",
            password="00000",
            host="localhost",
            port="54320",
            database="mydb"
        )
        return connection
    except Exception as error:
        print(f"Ошибка подключения к PostgreSQL: {error}")
        return None


# Функция для вставки данных в PostgreSQL
def insert_data_to_postgres(headers, data, connection):
    try:
        cursor = connection.cursor()

        # Очищаем имена колонок
        sanitized_headers = sanitize_column_names(headers)

        # Создание таблицы (если нужно)
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS my_table (
            {', '.join([f'{col} TEXT' for col in sanitized_headers])}
        );
        """
        cursor.execute(create_table_query)
        connection.commit()

        # Вставка данных
        insert_query = f"INSERT INTO my_table ({', '.join(sanitized_headers)}) VALUES %s"

        # Преобразование данных в формат для вставки
        formatted_data = [tuple(row) for row in data]

        psycopg2.extras.execute_values(cursor, insert_query, formatted_data)
        connection.commit()
        print("Данные успешно вставлены")
    except Exception as error:
        print(f"Ошибка при вставке данных: {error}")
    finally:
        cursor.close()


# Главная функция
def main():
    # URL файла
    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"

    # Шаг 1: Загрузка данных по URL
    csv_content = download_file(url)

    if csv_content:
        # Шаг 2: Обработка CSV данных
        headers, data = process_csv_data(csv_content)

        # Шаг 3: Подключение к базе данных
        connection = connect_to_db()

        if connection is not None:
            # Шаг 4: Вставка данных в PostgreSQL
            insert_data_to_postgres(headers, data, connection)

            # Закрытие соединения
            connection.close()


if __name__ == "__main__":
    main()
