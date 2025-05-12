# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy and make the entrypoint script executable
RUN chmod +x entrypoint.sh

# Make port 5000 available to the world outside the container
EXPOSE 5000

# Define environment variable for Flask app (using main-aux.py)
ENV FLASK_APP=main.py
ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_RUN_PORT=5000

# Run the entrypoint script
CMD ["./entrypoint.sh"]
