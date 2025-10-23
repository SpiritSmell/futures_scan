FROM python:3.12.2-slim
#COPY requirements.txt.bak .
# Создаем новую директорию внутри контейнера и копируем туда все файлы, кроме requirements.txt.bak
COPY requirements.txt /
RUN pip install -r /requirements.txt --trusted-host pypi.python.org

RUN mkdir /app
COPY . /app
WORKDIR /app

# set a default environment variable for the name of your bot
ENV config='./data/Z_I_CE10001_10.cfg'

# set the start command
CMD [ "python3", "./price_collector.py" ]
