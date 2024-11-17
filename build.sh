#!/bin/bash

# Exit script on any error
set -e

# Define variables
REPO_NAME="dgi-dash"
REGISTRY="jankar" # e.g., DockerHub, GitHub Container Registry
VERSION=$(date +"%Y.%m.%d.%H%M") # Use timestamp as version tag
BRANCH=$(git rev-parse --abbrev-ref HEAD)
COMMIT_HASH=$(git rev-parse HEAD) # Get current Git commit hash
LAST_BUILT_FILE=".last_built_commit" # File to track the last built commit hash

# Check if there are any uncommitted changes
if [[ -n $(git status --porcelain) ]]; then
    echo "You have uncommitted changes. Please commit or stash them before building."
    exit 1
fi

# Ensure the latest code is pulled from GitHub
git pull origin "$BRANCH"

# Check if the commit has already been built
if [[ -f "$LAST_BUILT_FILE" && $(cat "$LAST_BUILT_FILE") == "$COMMIT_HASH" ]]; then
    echo "No changes detected since the last build. Skipping build and push."
    exit 0
fi

# Update the version file
echo "$VERSION" > VERSION

# Commit the version file to Git
git add VERSION
git commit -m "Update version to $VERSION"
git push origin "$BRANCH"

# Build the Docker image with the commit hash as a build argument
docker build --build-arg COMMIT_HASH="$COMMIT_HASH" -t "$REGISTRY/$REPO_NAME:$VERSION" -t "$REGISTRY/$REPO_NAME:latest" .

# Push the image to the registry
docker push "$REGISTRY/$REPO_NAME:$VERSION"
docker push "$REGISTRY/$REPO_NAME:latest"

# Record the built commit hash
echo "$COMMIT_HASH" > "$LAST_BUILT_FILE"

echo "Build and push completed successfully!"
echo "Version: $VERSION"
echo "Commit Hash: $COMMIT_HASH"
