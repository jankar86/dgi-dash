#!/bin/bash

# Exit script on any error
set -e

# Define variables
REPO_NAME="dgi-dash"
REGISTRY="${REGISTRY:-jankar}" # Default registry
DATE=$(date +"%y%m%d")
COMMIT_HASH=$(git rev-parse --short HEAD)
VERSION="$DATE-$COMMIT_HASH"
BRANCH=$(git rev-parse --abbrev-ref HEAD)
LAST_BUILT_FILE=".last_built_commit"
RUN_CONTAINER=false # Default is to not run the container

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --run) RUN_CONTAINER=true ;; # Enable running the container locally
        --force) FORCE_BUILD=true ;; # Skip commit check
        *) echo "Unknown parameter: $1"; exit 1 ;;
    esac
    shift
done

# Change directory to the script's location
cd "$(dirname "$0")"

# Check if there are any uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo "You have uncommitted changes. Run 'git commit' or 'git stash' before building."
    exit 1
fi

# Ensure the latest code is pulled from GitHub
echo "Pulling latest changes from branch: $BRANCH..."
git pull origin "$BRANCH"

# Check if the commit has already been built
if [[ -z "$FORCE_BUILD" && -f "$LAST_BUILT_FILE" && $(cat "$LAST_BUILT_FILE") == "$COMMIT_HASH" ]]; then
    echo "No changes detected since the last build. Skipping build and push."
    exit 0
fi

# Update the version file
echo "$VERSION" > VERSION

# Commit the version file to Git
echo "Updating version file..."
git add VERSION
git commit -m "Update version to $VERSION"
git push origin "$BRANCH"

# Build the Docker image with the commit hash as a build argument
echo "Building Docker image for $REPO_NAME, version: $VERSION..."
docker build --build-arg COMMIT_HASH="$COMMIT_HASH" -t "$REGISTRY/$REPO_NAME:$VERSION" -t "$REGISTRY/$REPO_NAME:latest" .

# Push the image to the registry
echo "Pushing Docker image to $REGISTRY..."
docker push "$REGISTRY/$REPO_NAME:$VERSION"
docker push "$REGISTRY/$REPO_NAME:latest"

# Record the built commit hash
echo "$COMMIT_HASH" > "$LAST_BUILT_FILE"

# Optionally run the built image locally
if $RUN_CONTAINER; then
    echo "Running Docker container locally..."

    # Stop and remove any existing container with the same name
    CONTAINER_NAME="dgi-dash-local"
    if docker ps -a --format '{{.Names}}' | grep -Eq "^$CONTAINER_NAME\$"; then
        echo "Stopping and removing existing container $CONTAINER_NAME..."
        docker stop "$CONTAINER_NAME" && docker rm "$CONTAINER_NAME"
    fi

    # Run the new container
    docker run -d --name "$CONTAINER_NAME" -p 8080:8080 "$REGISTRY/$REPO_NAME:$VERSION"

    echo "Container is running locally on http://localhost:8080"
else
    echo "Skipping container run. Use --run to run the container locally."
fi

echo "Build, push, and optional run completed successfully!"
