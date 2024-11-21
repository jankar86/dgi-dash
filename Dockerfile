# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Install required packages including nano
RUN apt-get update && apt-get install -y \
    nano \
    bash \
    tini \
    && apt-get clean

# Set build argument for Git commit hash
ARG COMMIT_HASH

# Set up the application directory
WORKDIR /app

# Inject the commit hash into an environment variable
ENV GIT_COMMIT_HASH=$COMMIT_HASH

# Ensure the /app directory exists and write the commit hash to a file
RUN mkdir -p /app && echo $COMMIT_HASH > /app/commit_hash.txt
RUN apt-get update && apt-get install -y tini && apt-get clean


# Install dependencies
COPY /app/requirements.txt .
RUN pip install -r requirements.txt

# Copy the application code
COPY app/* /app/.

# Set tini as the entrypoint
ENTRYPOINT ["/usr/bin/tini", "--"]

# Expose port and run the app
EXPOSE 8080
#CMD ["python", "app.py"]
CMD ["bash", "-c", "python ui/app.py & python ingest/ingestion.py"]

# Set bash as the default shell
SHELL ["/bin/bash", "-c"]