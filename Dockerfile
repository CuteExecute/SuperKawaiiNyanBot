FROM python:3

WORKDIR /app

COPY . .

RUN pip install pytest \
    pip install sqlalchemy==1.4.41

ENTRYPOINT [ "pytest" ]
