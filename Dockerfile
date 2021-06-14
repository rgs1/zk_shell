FROM python:buster

WORKDIR zk-shell

COPY requirements.txt ./

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT [ "python", "./bin/zk-shell"]