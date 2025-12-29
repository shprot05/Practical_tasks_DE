import json
from db_connection import get_connection
from abc import ABC, abstractmethod


class DataLoader(ABC):
    def __init__(self):
        self.conn = get_connection()
        self.cursor = self.conn.cursor()

    @abstractmethod
    def load_data(self, path: str):
        pass


class RoomLoader(DataLoader):
    def load_data(self, path: str):
        with open(path, "r", encoding="utf-8") as f:
            rooms_data = json.load(f)

        for room in rooms_data:
            self.cursor.execute(
                "INSERT INTO rooms (id, name) VALUES (%s, %s)  ON CONFLICT (id) DO NOTHING",
                (room["id"], room["name"]),
            )
        self.conn.commit()


class StudentLoader(DataLoader):
    def load_data(self, path:str):
        with open(path, "r", encoding="utf-8") as f:
            students_data = json.load(f)

        for student in students_data:
            self.cursor.execute(
                "INSERT INTO students (id, name, birthday, room, sex) VALUES (%s, %s,%s, %s,%s)  ON CONFLICT (id) DO NOTHING",
                (
                    student["id"],
                    student["name"],
                    student["birthday"],
                    student["room"],
                    student["sex"],
                ),
            )
        self.conn.commit()
