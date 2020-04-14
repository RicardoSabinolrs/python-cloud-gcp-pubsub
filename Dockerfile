FROM python:3.7-slim

RUN python -m pip install --upgrade pip
RUN pip install pipenv

WORKDIR /app
COPY . /app

RUN pipenv install --system --deploy --ignore-pipfile

ENV PYTHONUNBUFFERED True

CMD ["python", "main.py"]