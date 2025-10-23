import clickhouse_connect
import uuid
import datetime
import pyarrow as pa

class ClickHouseLoader:
    def __init__(self, host, database, username='default', password=''):
        self.client = clickhouse_connect.get_client(
            host=host, port=8123, username=username, password=password, database=database)

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
        return 'String'

    def flatten_dict(self, d, parent_key='', sep='__'):
        """Рекурсивно уплощает вложенные словари"""
        items = {}
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.update(self.flatten_dict(v, new_key, sep=sep))
            else:
                items[new_key] = v
        return items

    def unflatten_dict(self, d, sep='__'):
        """Разворачивает уплощённый словарь обратно в вложенный формат"""
        result = {}
        for key, value in d.items():
            parts = key.split(sep)
            d_ref = result
            for part in parts[:-1]:
                if part not in d_ref:
                    d_ref[part] = {}
                d_ref = d_ref[part]
            d_ref[parts[-1]] = value
        return result

    def generate_structure_description(self, data):
        """Генерирует описание структуры данных"""
        flat_data = self.flatten_dict(data)
        description = {key: self.detect_type(value) for key, value in flat_data.items()}
        return description

    def create_table(self, name, columns):
        """Создаёт таблицу в ClickHouse"""
        columns_sql = ', '.join([f"{col} {dtype}" for col, dtype in columns.items()])
        sql = f"""
        CREATE TABLE IF NOT EXISTS {name} (
            id UUID DEFAULT generateUUIDv4(),
            {columns_sql}
        ) ENGINE = MergeTree()
        ORDER BY id
        """
        self.client.command(sql)

    def insert_data(self, table, data):
        """Вставляет одну запись в таблицу с использованием Arrow"""
        if not data:
            return
        data['id'] = str(uuid.uuid4())
        schema = pa.schema([(key, self.pyarrow_type(value)) for key, value in data.items()])
        table_arrow = pa.Table.from_pydict({k: [v] for k, v in data.items()}, schema=schema)
        self.client.insert_arrow(table, table_arrow)

    def insert_multiple_data(self, table, data_list):
        """Вставляет список словарей в таблицу с использованием Arrow"""
        if not data_list:
            return
        for data in data_list:
            data['id'] = str(uuid.uuid4())
        schema = pa.schema([(key, self.pyarrow_type(data_list[0][key])) for key in data_list[0].keys()])
        table_arrow = pa.Table.from_pydict({k: [d[k] for d in data_list] for k in data_list[0].keys()}, schema=schema)
        self.client.insert_arrow(table, table_arrow)

    def pyarrow_type(self, value):
        """Определяет тип данных для PyArrow"""
        if isinstance(value, bool):
            return pa.bool_()
        elif isinstance(value, int):
            return pa.int64()
        elif isinstance(value, float):
            return pa.float64()
        elif isinstance(value, str):
            return pa.string()
        elif isinstance(value, datetime.date):
            return pa.date32()
        elif isinstance(value, datetime.datetime):
            return pa.timestamp('s')
        elif isinstance(value, list):
            if value:
                return pa.list_(self.pyarrow_type(value[0]))
            return pa.list_(pa.string())
        return pa.string()

    def read_and_unflatten_data(self, table):
        """Читает данные из таблицы и разворачивает их в исходный формат"""
        result = self.client.query(f"SELECT * FROM {table}").result_rows
        columns = self.client.query(f"DESCRIBE TABLE {table}").result_rows
        column_names = [col[0] for col in columns if col[0] != 'id']

        unflattened_data = []
        for row in result:
            flat_data = {col: row[idx + 1] for idx, col in enumerate(column_names)}  # Пропускаем id
            flat_data['id'] = row[0]  # Добавляем id
            unflattened_data.append(self.unflatten_dict(flat_data))
        return unflattened_data

    def delete_table(self, table_name):
        """Удаляет таблицу из ClickHouse"""
        sql = f"DROP TABLE IF EXISTS {table_name}"
        self.client.command(sql)

    def delete_record(self, table, record_id):
        """Удаляет запись из таблицы по UUID"""
        sql = f"ALTER TABLE {table} DELETE WHERE id = '{record_id}'"
        self.client.command(sql)

# Пример использования:
if __name__ == '__main__':
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

    # Создание таблицы
    loader.create_table('product_data', structure_description)

    # Вставка данных
    flat_data = loader.flatten_dict(data)
    loader.insert_data('product_data', flat_data)

    # Вставка списка данных
    data_list = [flat_data.copy() for _ in range(2000)]
    loader.insert_multiple_data('product_data', data_list)

    # Чтение и разворачивание данных
    retrieved_data = loader.read_and_unflatten_data('product_data')
    print(retrieved_data)

    # Удаление записи (пример с первым UUID из таблицы)
    first_record_id = retrieved_data[0]['id']
    loader.delete_record('product_data', first_record_id)

    # Удаление таблицы
    loader.delete_table('product_data')
