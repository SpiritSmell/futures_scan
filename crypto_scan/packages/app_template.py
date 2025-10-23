import sys
import getopt
import json
import os

import logging

from pathlib import Path


# Get the logger for the current module. Configuration is handled by the main script.
logger = logging.getLogger(__name__)

MAX_REQUEST_SIZE = 41943040


def setup_logging():
    # Настройка логирования
    logging.basicConfig(filename='prices_price_collector.log', level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    # Добавление обработчика для вывода логов в консоль
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(console_formatter)
    logging.getLogger('').addHandler(console_handler)


class AppTemplate:
    def __init__(self, expected_parameters=[]):
        # Создание экземпляра класса AppTemplate с ожидаемыми параметрами
        # Параметр [('config=', "<config filename>")] означает, что при выполнении
        # функций get_arguments_from_env() и get_arguments() объект будет искать в
        # переменных окружения параметр с именем config и если найдет, добавит его
        # имя и значение в словарь parameters
        #  Если потребуется добавить еще один параметр, который программа будет
        #  брать из командной строки, можно это сделать следующим образом:
        # at = AppTemplate([('config=', "<config filename>"),('env=',"Описание синтаксиса и смысла параметра env")])
        # и так далее.
        self.settings = {}
        self.parameters = {}
        if expected_parameters:
            self.expected_parameters = [item[0] for item in expected_parameters]
            self.expected_parameters_description = [item[1] if len(item) > 1 else None for item in expected_parameters]
            self.expected_parameters_conversion = [item[2] if len(item) > 2 else None for item in expected_parameters]

    @staticmethod
    def file_exist(file_name):
        # Проверка существования файла
        file_path = os.path.abspath(file_name)
        if Path(file_path).is_file():
            logger.info(f"File {file_name} exists.")
            return True
        else:
            logger.info("File does not exist.")
            return False

    def print_help(self):
        # Вывод справки о параметрах
        logger.info("Usage: extract_items.py [parameters]")
        logger.info("")
        logger.info("Parameters:")
        logger.info(f'--help print this message')
        for index, parameter in enumerate(self.expected_parameters):
            logger.info(f'--{parameter} {self.expected_parameters_description[index]}')
        logger.info("")
        logger.info("Example:")
        logger.info("extract_items.py -c /path/to/extract_items.cfg")

    def convert_value(self, value, value_type):
        if value_type == 'str':
            return value
        if value_type == 'bool':
            return value == "True"
        if value_type == 'int':
            return int(value)
        if value_type == 'float':
            return float(value)
        return value

    def get_arguments_from_env(self):
        # Получение параметров из окружения
        for i, expected_parameter in enumerate(self.expected_parameters):
            value = os.getenv(expected_parameter.strip('='))
            if not value:
                continue
            value = value.lstrip('"').rstrip('"')
            if self.expected_parameters_conversion[i]:
                value = self.convert_value(value, self.expected_parameters_conversion[i])
            #value = self.expected_parameters_conversion[i](value)
            self.parameters[expected_parameter.strip('=')] = value

    def get_arguments(self):
        # Получение параметров из командной строки
        short_options = ""
        long_options = ["help"] + self.expected_parameters
        expected_arguments = []
        for variable in self.expected_parameters:
            expected_arguments.append("--" + variable.strip('='))

        received_arguments, values = getopt.getopt(sys.argv[1:], short_options, long_options)

        for current_argument, current_value in received_arguments:
            if current_argument in ("-h", "--help"):
                self.print_help()
            elif current_argument in expected_arguments:
                self.parameters[current_argument.strip('--')] = current_value
                logger.info(f"Variable {current_argument} = {current_value}")

        for value in values:
            logger.info("Extra arguments:", value)

    @staticmethod
    def save_json_to_file(data, filename):
        # Сохранение JSON в файл
        try:
            with open(filename, 'w', encoding="utf-8") as file:
                json.dump(data, file, indent=4, ensure_ascii=False)
            logger.info(f"JSON data saved to {filename}")
        except Exception as e:
            logger.error(f"Error saving JSON data to {filename}: {e}")

    @staticmethod
    def load_json_from_file(filename):
        # Загрузка JSON из файла
        try:
            with open(filename, 'r', encoding="utf-8") as file:
                data = json.load(file)
            return data
        except Exception as e:
            logger.error(f"Error loading JSON data from {filename}: {e}")
            return None

    def load_settings_from_file(self, config_file_name=None):
        try:
            # Получение пути к файлу конфигурации
            if not config_file_name:
                config_file_name = self.parameters['config']
            # Проверка наличия пути к файлу
            if not config_file_name:
                raise ValueError("Config path not provided")
            # Проверка наличия файла
            if not self.file_exist(config_file_name):
                raise FileNotFoundError(f"Config file {config_file_name} does not exist in {os.getcwd()}")

            # Загрузка настроек из файла
            self.settings = self.load_json_from_file(config_file_name)
            logger.info(f" Loaded settings {self.settings}")
            if not self.settings:
                raise ValueError("Error loading settings from config file")
        except (ValueError, FileNotFoundError) as e:
            logger.error(f"Error loading settings: {e}")
            sys.exit(2)


def main():
    try:
        # Создание экземпляра класса AppTemplate с ожидаемыми параметрами
        at = AppTemplate([('config=', "<config filename>")])
        # Получение параметров из окружения и командной строки
        at.get_arguments_from_env()
        at.get_arguments()
        # Загрузка настроек из файла
        at.load_settings_from_file(at.parameters['config'])
    except KeyboardInterrupt:
        logger.info("Script execution interrupted by user")
    except Exception as e:
        logger.error(f"An error occurred: {e}")


if __name__ == '__main__':
    # Настройка логирования
    # setup_logging()
    # Вызов функции main для выполнения скрипта
    main()
