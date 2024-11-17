# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set build argument for Git commit hash
ARG COMMIT_HASH

# Inject the commit hash into an environment variable
ENV GIT_COMMIT_HASH=$COMMIT_HASH

# Optionally write the commit hash to a file
RUN echo $COMMIT_HASH > /app/commit_hash.txt

# Install dependencies
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy the application
COPY . .

# Expose port and run the app
EXPOSE 5000
CMD ["python", "app.py"]

