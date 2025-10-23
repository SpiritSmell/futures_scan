import clickhouse_connect
import uuid
import datetime

class ClickHouseLoader:
    def __init__(self, host, database, username='default', password=''):
        self.client = clickhouse_connect.get_client(
            host=host, port=8123, username=username, password=password, database=database)
        self.structure_description = {}

    def detect_type(self, value):
        """Определяет тип данных для ClickHouse"""
        if isinstance(value, bool):
            return 'UInt8'
        elif isinstance(value, int):
            return 'Int64'
        elif isinstance(value, float):
            return 'Float64'
        elif isinstance(value, str):
            return 'String'
        elif isinstance(value, datetime.date):
            return 'Date'
        elif isinstance(value, datetime.datetime):
            return 'DateTime'
        elif isinstance(value, list):
            if value:
                elem_type = self.detect_type(value[0])
                return f"Array({elem_type})"
            return "Array(String)"
        elif isinstance(value, dict):
            return 'UUID'  # Для вложенных структур
        return 'String'

    def generate_structure_description(self, data):
        """Генерирует описание структуры данных"""
        description = {}
        for key, value in data.items():
            if isinstance(value, dict):
                description[key] = self.generate_structure_description(value)
            else:
                description[key] = self.detect_type(value)
        return description

    def create_table(self, name, columns):
        """Создаёт таблицу в ClickHouse"""
        columns_sql = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
        sql = f"""
        CREATE TABLE IF NOT EXISTS {name} (
            id UUID DEFAULT generateUUIDv4(),
            {columns_sql},
            parent_id UUID NULL
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        self.client.command(sql)

    def insert_data(self, table, data, parent_id=None):
        """Вставляет данные в таблицу"""
        if not data:
            return
        columns = list(data.keys())
        values = [str(uuid.uuid4())] + list(data.values()) + [parent_id]
        placeholders = ', '.join(['%s'] * len(values))

        sql = f"INSERT INTO {table} ({', '.join(['id'] + columns + ['parent_id'])}) VALUES ({placeholders})"
        self.client.query(sql, values)

    def process_dict(self, data, parent_id=None):
        """Рекурсивно разбирает словарь"""
        columns = {}
        child_tables = {}
        data_for_insert = {}

        for key, value in data.items():
            if isinstance(value, dict):
                child_tables[key] = value
            else:
                columns[key] = type(value).__name__
                data_for_insert[key] = value

        return data_for_insert, child_tables, columns

    def create_tables_from_structure(self, structure, parent_table=None):
        """Создаёт таблицы на основе структуры данных"""
        if parent_table is None:
            parent_table = "root_table"

        columns = {}
        for key, value in structure.items():
            if isinstance(value, dict):
                columns[key] = "UUID"
            else:
                columns[key] = value

        self.create_table(parent_table, columns)

        for key, value in structure.items():
            if isinstance(value, dict):
                self.create_tables_from_structure(value, f"{parent_table}_{key}")

    def prepare_data_for_insertion(self, data, structure_description):
        """Подготовка данных для вставки"""
        data_for_insert = {}
        child_tables = {}

        for key, value in data.items():
            if key in structure_description:
                expected_type = structure_description[key]
                if isinstance(value, dict):
                    child_tables[key] = value
                else:
                    data_for_insert[key] = value

        return data_for_insert, child_tables

    def create_tables_from_structure(self, structure, parent_table="root_table"):
        """Создает таблицы на основе структуры данных (без вставки данных)"""
        # Создаем таблицу для текущего уровня (если это не вложенная структура)
        columns = {}
        for key, value in structure.items():
            if isinstance(value, dict):
                columns[key] = "UUID"  # Для вложенных структур столбцы будут UUID
            else:
                columns[key] = value  # Используем уже готовое описание типа

        # Создаем таблицу для текущего уровня
        self.create_table(parent_table, columns)

        # Рекурсивно создаем таблицы для вложенных объектов
        for key, value in structure.items():
            if isinstance(value, dict):
                self.create_tables_from_structure(value, f"{parent_table}_{key}")

    def insert_data_into_tables(self, data, structure_description=None, parent_id=None, parent_table="root_table"):
        """Вставляет данные в таблицы, основываясь на описании структуры"""
        if structure_description is None:
            structure_description = self.generate_structure_description(data)

        # Подготовка данных для вставки в текущую таблицу
        data_for_insert, child_tables = self.prepare_data_for_insertion(data, structure_description)

        # Вставляем данные в текущую таблицу
        self.insert_data(parent_table, data_for_insert, parent_id)

        # Рекурсивно обрабатываем вложенные таблицы и вставляем данные в дочерние таблицы
        for key, value in child_tables.items():
            child_table_name = f"{parent_table}_{key}"
            new_parent_id = str(uuid.uuid4())  # Уникальный ID для дочерней таблицы
            # Вставляем данные в дочернюю таблицу
            self.insert_data_into_tables(value, structure_description.get(key), new_parent_id, child_table_name)
            # Вставляем ID дочерней таблицы в родительскую таблицу
            self.insert_data(child_table_name, {"id": new_parent_id}, parent_id)


# Пример использования:
loader = ClickHouseLoader(host='192.168.192.42', database='mart')

# Пример данных
data = {
    "name": "Product A",
    "price": 99.99,
    "is_available": True,
    "release_date": datetime.date(2023, 5, 10),
    "created_at": datetime.datetime(2024, 2, 4, 15, 30),
    "tags": ["electronics", "gadget"],
    "details": {
        "manufacturer": "Company X",
        "warranty": "2 years",
        "specifications": {
            "weight": 1.5,
            "dimensions": ["10cm", "20cm", "5cm"]
        }
    }
}

# Генерация описания структуры данных
structure_description = loader.generate_structure_description(data)

# Сначала создаем таблицы
loader.create_tables_from_structure(structure_description)

# Теперь вставляем данные в таблицы
loader.insert_data_into_tables(data, structure_description)
