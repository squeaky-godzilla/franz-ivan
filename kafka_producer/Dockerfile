FROM frolvlad/alpine-python3

COPY requirements.txt /

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

COPY produce.py /app/
WORKDIR /app

CMD ["python3", "produce.py", "--envvars"]