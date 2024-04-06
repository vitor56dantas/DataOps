FROM apache/airflow:2.2.4-python3.9

WORKDIR /usr/src
COPY . /usr/src/

RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt
RUN pip install -r requirements.txt
RUN pip install mysql-connector-python
RUN pip install SQLAlchemy