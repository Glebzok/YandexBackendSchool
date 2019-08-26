FROM python:3.7.4
RUN apt-get -q update && apt-get -qy install netcat=1.10-41.1
WORKDIR /code
COPY ./requirements.txt /code
RUN pip install -r requirements.txt
COPY . /code
CMD ["/code/start.sh"]

