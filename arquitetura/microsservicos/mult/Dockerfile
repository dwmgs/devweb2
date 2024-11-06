# using a python small basic image
FROM python:alpine

# exposing our app port in docker internal network
EXPOSE 5000

# creates a dir for our application
WORKDIR /mult

# copy our requirements.txt file and install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy the rest of our application
COPY . .

# Executa o servidor FastAPI com Uvicorn
CMD ["fastapi", "run", "app.py", "--port", "5000"]