import os
import json
import xml.etree.ElementTree as ET


# Класс для экспорта данных в JSON или XML
class Exporter():
    def __init__(self, export_format, path_to_export):
        self.export_format = export_format
        self.path_to_export = path_to_export

    # Экспорт в JSON
    def export_to_json(self, data, filename: str) -> None:
        output_dir = os.path.join(self.path_to_export, 'json_output')
        os.makedirs(output_dir, exist_ok=True)
        with open(os.path.join(output_dir, filename), 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    # Экспорт в XML
    def export_to_xml(self, data, filename: str, root_tag: str ="items", item_tag:str ="item") -> None:
        output_dir = os.path.join(self.path_to_export, 'xml_output')
        os.makedirs(output_dir, exist_ok=True)
        root = ET.Element(root_tag)
        for entry in data:
            item_elem = ET.SubElement(root, item_tag)
            if isinstance(entry, dict):
                for key, value in entry.items():
                    sub_elem = ET.SubElement(item_elem, key)
                    sub_elem.text = str(value)
            else:
                item_elem.text = str(entry)
        tree = ET.ElementTree(root)
        tree.write(os.path.join(output_dir, filename), encoding='utf-8', xml_declaration=True)

