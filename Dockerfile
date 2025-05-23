# Use an official Python runtime as a parent image
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy and make the entrypoint script executable
RUN chmod +x entrypoint.sh

# Create .env file with configuration variables
RUN echo "DB_HOST=your-mysql-host.example.com" >> .env && \
    echo "DB_PORT=3306" >> .env && \
    echo "DB_USER=your_username" >> .env && \
    echo "DB_PASSWORD=your_password" >> .env && \
    echo "DB_NAME=your_database_name" >> .env && \
    echo "JWT_SECRET_KEY=your_jwt_secret_key" >> .env

# Make port 8080 available to the world outside the container
EXPOSE 8080

# Define essential environment variables for Flask
ENV FLASK_APP=main.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=8080
ENV LEDGER_URL=http://13.61.7.161:8000

# Run the entrypoint script
CMD ["./entrypoint.sh"]
