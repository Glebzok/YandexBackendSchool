FROM python:3.4.3
WORKDIR /code
COPY ./requirements.txt /code
RUN pip install -r requirements.txt
COPY . /code
CMD ["python", "db_init.py"]
CMD ["python", "api.py"]
