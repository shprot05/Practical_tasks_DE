import unittest
import os
import json
import tempfile
from export_data import Exporter
import xml.etree.ElementTree as ET

class TestExporter(unittest.TestCase):
    def setUp(self):
        # Создаём временную директорию для экспорта
        self.test_dir = tempfile.TemporaryDirectory()
        self.exporter = Exporter("json", self.test_dir.name)

    def tearDown(self):
        # Удаляем временную директорию после теста
        self.test_dir.cleanup()

    def test_export_to_json_creates_file_with_correct_content(self):
        data = [{"room": "101", "count": 5}]
        filename = "test.json"
        self.exporter.export_to_json(data, filename)

        # Проверяем, что файл создан
        file_path = os.path.join(self.test_dir.name, "json_output", filename)
        self.assertTrue(os.path.exists(file_path))

        # Проверяем содержимое файла
        with open(file_path, encoding="utf-8") as f:
            content = json.load(f)
        self.assertEqual(content, data)

    def test_export_to_xml_creates_file_with_correct_content(self):
        # Переключаем формат экспорта на XML
        self.exporter.export_format = "xml"
        data = [{"room": "101", "count": 5}]
        filename = "test.xml"
        self.exporter.export_to_xml(data, filename, root_tag="rooms", item_tag="room")

        # Проверяем, что файл создан
        file_path = os.path.join(self.test_dir.name, "xml_output", filename)
        self.assertTrue(os.path.exists(file_path))

        # Загружаем и проверяем структуру XML
        tree = ET.parse(file_path)
        root = tree.getroot()
        self.assertEqual(root.tag, "rooms")

        rooms = root.findall("room")
        self.assertEqual(len(rooms), 1)

        room_elem = rooms[0]
        self.assertEqual(room_elem.find("room").text, "101")
        self.assertEqual(room_elem.find("count").text, "5")
