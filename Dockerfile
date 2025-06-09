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

# Create a script to generate .env file from environment variables
# Don't forget to set the environment variables in your Docker run command or Docker Compose file
# Example: docker run -e DB_HOST=localhost -e DB_PORT=5432 -e DB_USER=user -e DB_PASSWORD=pass -e DB_NAME=db -e JWT_SECRET_KEY=secret
RUN echo '#!/bin/bash' > /app/generate_env.sh && \
    echo 'echo "DB_HOST=$DB_HOST" > .env' >> /app/generate_env.sh && \
    echo 'echo "DB_PORT=$DB_PORT" >> .env' >> /app/generate_env.sh && \
    echo 'echo "DB_USER=$DB_USER" >> .env' >> /app/generate_env.sh && \
    echo 'echo "DB_PASSWORD=$DB_PASSWORD" >> .env' >> /app/generate_env.sh && \
    echo 'echo "DB_NAME=$DB_NAME" >> .env' >> /app/generate_env.sh && \
    echo 'echo "JWT_SECRET_KEY=$JWT_SECRET_KEY" >> .env' >> /app/generate_env.sh && \
    echo 'echo "LEDGER_URL=$LEDGER_URL" >> .env' >> /app/generate_env.sh && \
    chmod +x /app/generate_env.sh

# Make port 8080 available to the world outside the container
EXPOSE $FLASK_RUN_PORT

# Update entrypoint script to first generate the .env file and then run the app
CMD ["/bin/bash", "-c", "/app/generate_env.sh && ./entrypoint.sh"]
