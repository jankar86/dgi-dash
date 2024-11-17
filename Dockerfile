# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set build argument for Git commit hash
ARG COMMIT_HASH

# Set up the application directory
WORKDIR /app

# Inject the commit hash into an environment variable
ENV GIT_COMMIT_HASH=$COMMIT_HASH

# Ensure the /app directory exists and write the commit hash to a file
RUN mkdir -p /app && echo $COMMIT_HASH > /app/commit_hash.txt

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code
COPY . .

# Expose port and run the app
EXPOSE 5000
CMD ["python", "app.py"]
