# Use an official Python image as a base (in this case, Python 3.10)
FROM python:3.10-slim

# Install PostgreSQL client (psql)
RUN apt-get update && apt-get install -y postgresql-client

# Set the working directory inside the container
WORKDIR /app

# Copy the project files into the container
COPY . /app

# Upgrade pip and setuptools
RUN pip install --upgrade pip setuptools

# Install the necessary dependencies
RUN pip install pandas psycopg2-binary sqlalchemy python-dotenv

# Define the entry point of the application 
CMD ["python", "scripts/python/app.py"]
