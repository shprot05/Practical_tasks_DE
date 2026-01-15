from db_connection import get_connection


# Создание индексов для ускорения SQL-запросов
def create_indexes() -> None:
    conn = get_connection()
    cursor = conn.cursor()
    # Индекс по комнате студента
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_room_id ON students(room);")
    # Индекс по дате рождения студента
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_birthday ON students(birthday);")
    # Индекс по полу студента
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_student_sex ON students(sex);")
    # Индекс по названию комнаты
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_room_name ON rooms(name);")
    conn.commit()


# Словарь с SQL-запросами
queries = {
    # Количество студентов в каждой комнате
    1: """SELECT rooms.name as room_number, COUNT(*) as amount_of_students
          FROM rooms
                   JOIN students ON rooms.id = students.room
          GROUP BY rooms.name
          ORDER BY amount_of_students""",

    # Средний возраст студентов по комнатам (топ 5 с самым низким возрастом)
    2: """SELECT rooms.name as room_number,
                 ROUND(AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))), 2) AS students_avg_age
          FROM rooms
                   JOIN students ON rooms.id = students.room
          GROUP BY room_number
          ORDER BY students_avg_age LIMIT 5""",

    # Разница в возрасте между самым старшим и самым младшим студентом в комнате (топ 5)
    3: """SELECT rooms.name as room_number,
                 MAX(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) -
                 MIN(EXTRACT(YEAR FROM AGE(CURRENT_DATE, students.birthday))) AS students_age_difference
          FROM rooms
                   INNER JOIN students ON rooms.id = students.room
          GROUP BY rooms.id, room_number
          ORDER BY students_age_difference DESC 
		  LIMIT 5""",

    # Комнаты, где все студенты — мужчины
    4: """SELECT rooms.name as room_number
          FROM rooms
                   INNER JOIN students ON rooms.id = students.room
          GROUP BY rooms.id, rooms.name
          HAVING COUNT(DISTINCT students.sex) = 1
             AND MAX(students.sex) = 'M'""",

    # Комнаты, где все студенты — женщины
    5: """SELECT rooms.name as room_number
          FROM rooms
                   INNER JOIN students ON rooms.id = students.room
          GROUP BY rooms.id, rooms.name
          HAVING COUNT(DISTINCT students.sex) = 1
             AND MAX(students.sex) = 'F'"""
}


# Класс для выполнения SQL-запросов и экспорта результатов
class SqlExecuter():
    def __init__(self, exporter):
        self.queries = queries
        self.conn = get_connection()
        self.cursor = self.conn.cursor()
        self.exporter = exporter

    def close(self) -> None:
        self.cursor.close()
        self.conn.close()

    # 1 Запрос: количество студентов в каждой комнате
    def students_count_in_rooms(self) -> None:
        self.cursor.execute(self.queries[1])
        result1 = self.cursor.fetchall()
        output = [{"room_number": room, "amount_of_students": round(count, 2)} for room, count in result1]
        if self.exporter.export_format == "json":
            self.exporter.export_to_json(output, "students_count_in_rooms.json")
        else:
            self.exporter.export_to_xml(output, "students_count_in_rooms.xml", root_tag="rooms", item_tag="room")

    # 2 Запрос: комнаты с самым низким средним возрастом студентов
    def rooms_with_min_age(self) -> None:
        self.cursor.execute(self.queries[2])
        result2 = self.cursor.fetchall()
        output = [{"room_number": room_name, "students_avg_age": round(float(avg), 2)} for room_name, avg in result2]
        if self.exporter.export_format == "json":
            self.exporter.export_to_json(output, "rooms_with_min_age.json")
        else:
            self.exporter.export_to_xml(output, "rooms_with_min_age.xml", root_tag="rooms", item_tag="room")

    # 3 Запрос: комнаты с максимальной разницей в возрасте студентов
    def rooms_with_max_age_diff(self) -> None:
        self.cursor.execute(self.queries[3])
        result3 = self.cursor.fetchall()
        output = [{"room_number": room, "students_age_difference": round(float(diff), 2)} for room, diff in result3]
        if self.exporter.export_format == "json":
            self.exporter.export_to_json(output, "rooms_with_max_age_diff.json")
        else:
            self.exporter.export_to_xml(output, "rooms_with_max_age_diff.xml", root_tag="rooms", item_tag="room")

    # 4 Запрос: комнаты, где все студенты — мужчины
    def male_rooms(self) -> None:
        self.cursor.execute(self.queries[4])
        result4 = self.cursor.fetchall()
        rooms = [room_name[0] for room_name in result4]
        if self.exporter.export_format == "json":
            self.exporter.export_to_json(rooms, "male_rooms.json")
        else:
            self.exporter.export_to_xml(rooms, "male_rooms.xml", root_tag="rooms", item_tag="room")

    # 5 Запрос: комнаты, где все студенты — женщины
    def female_rooms(self) -> None:
        self.cursor.execute(self.queries[5])
        result5 = self.cursor.fetchall()
        rooms = [room_name[0] for room_name in result5]
        if self.exporter.export_format == "json":
            self.exporter.export_to_json(rooms, "female_rooms.json")
        else:
            self.exporter.export_to_xml(rooms, "female_rooms.xml", root_tag="rooms", item_tag="room")