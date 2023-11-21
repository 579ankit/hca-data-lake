FROM python:3.10-slim

ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN pip install --no-cache-dir -r requirements.txt 

# Set the command to run the application
CMD exec gunicorn --bind :$PORT --workers 3 --threads 10 --timeout 800 main:app