FROM python:3.7.4
WORKDIR /code
COPY ./requirements.txt /code
RUN pip install -r requirements.txt
COPY . /code
CMD ["python", "api.py"]
