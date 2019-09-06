FROM frolvlad/alpine-python3

RUN apk update && apk add postgresql-dev gcc python3-dev musl-dev

COPY requirements.txt /

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY produce.py /app/
WORKDIR /app

CMD ["python3", "produce.py", "--envvars"]