import os
import csv
import psycopg2


def parse_from_csv_to_bd() -> None:
    """
    Функция считывает папку (north_data)
    перебирает каждый файл и записывает данные
    в базу данных (PostgreSQL)
    """
    for filename in os.listdir('north_data'):
        with psycopg2.connect(
                host="localhost",
                database="north",
                user="postgres",
                password="12345"
        ) as connection:

            with connection.cursor() as cursor:
                with open(f'north_data\\{filename}') as csv_file:
                    header = next(csv_file)
                    csv_reader = csv.reader(csv_file)
                    for row in csv_reader:
                        values = "%s, " * (len(row))

                        query = (f"INSERT INTO {filename.split("_")[0]} "
                                 f"VALUES ({values[:-2]})")

                        cursor.execute(query, row)

    connection.close()


if __name__ == '__main__':
    parse_from_csv_to_bd()
