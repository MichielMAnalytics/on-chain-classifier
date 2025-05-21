
FROM python:3.10-slim-buster 

WORKDIR /app

COPY requirements.txt . 
RUN pip install --upgrade pip && pip install --no-cache-dir -r requirements.txt

COPY . . 

CMD sh -c 'python ./main.py; status=$?; echo "Exit status: $status"; exit $status'
