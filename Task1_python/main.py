from load_data import RoomLoader, StudentLoader
from queries import SqlExecuter, create_indexes
from export_data import Exporter
from create_tables import create_tables
import typer


# Основная функция, запускающая загрузку, обработку и экспорт данных
def main(
    path_to_students: str = typer.Option(..., prompt="Введите путь к json файлу студентов"),
    path_to_rooms: str = typer.Option(..., prompt="Введите путь к json файлу комнат"),
    export_format: str = typer.Option(..., prompt="В каком формате сохранить результаты? (json / xml)"),
    path_to_export: str = typer.Option(..., prompt="Введите путь для экспорта")
    ) -> None:
    room_loader = RoomLoader()
    student_loader = StudentLoader()


    create_tables()

    print("Вызов метода load_data для загрузки данных в бд")
    room_loader.load_data(path_to_rooms)
    student_loader.load_data(path_to_students)
    print("Данные загружены в бд")

    print("Выполнение запросов")
    exporter = Exporter(export_format, path_to_export)
    executer = SqlExecuter(exporter)

    # Создание индексов для ускорения запросов
    create_indexes()

    # Выполнение всех запросов
    executer.students_count_in_rooms()
    executer.rooms_with_min_age()
    executer.rooms_with_max_age_diff()
    executer.male_rooms()
    executer.female_rooms()

    executer.close() # закрываем соединение с бд

    print("Результаты запросов сохранены")

# Точка входа
if __name__ == "__main__":
    typer.run(main)




