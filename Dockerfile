From python:3.11
WORKDIR /code
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt
COPY ./main.py /code/app/main.py
CMD ["fastapi", "run", "app/main.py", "--port", "80"]
