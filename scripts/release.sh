#!/bin/bash
set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.0.2"
    echo "Current version: $(grep 'const Version' version.go | cut -d'\"' -f2)"
    exit 1
fi

NEW_VERSION=$1
CURRENT_VERSION=$(grep 'const Version' version.go | cut -d'"' -f2)

# Validate version format
if ! [[ $NEW_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo "❌ Invalid version format. Use MAJOR.MINOR.PATCH (e.g., 0.0.2)"
    exit 1
fi

echo "🚀 Releasing GoQueue v$NEW_VERSION"
echo "📦 Current version: $CURRENT_VERSION"
echo "📦 New version: $NEW_VERSION"
echo ""

# Check if we're on main branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$CURRENT_BRANCH" != "main" ]; then
    echo "❌ Must be on main branch to release. Current branch: $CURRENT_BRANCH"
    exit 1
fi

# Check if working directory is clean
if ! git diff-index --quiet HEAD --; then
    echo "❌ Working directory is not clean. Commit or stash changes first."
    exit 1
fi

# Update version.go
echo "📝 Updating version.go..."
sed -i "s/const Version = \"$CURRENT_VERSION\"/const Version = \"$NEW_VERSION\"/" version.go

# Update README.md
echo "📝 Updating README.md..."
sed -i "s/Version-$CURRENT_VERSION-green/Version-$NEW_VERSION-green/" README.md

# Run tests
echo "🧪 Running tests..."
if ! go test ./...; then
    echo "❌ Tests failed! Fix tests before releasing."
    # Revert changes
    git checkout version.go README.md
    exit 1
fi

# Run linter
echo "🔍 Running linter..."
if ! golangci-lint run; then
    echo "❌ Linting failed! Fix linting issues before releasing."
    # Revert changes
    git checkout version.go README.md
    exit 1
fi

# Commit changes
echo "📤 Committing changes..."
git add version.go README.md
git commit -m "Release v$NEW_VERSION"

# Create tag
echo "🏷️ Creating tag v$NEW_VERSION..."
git tag "v$NEW_VERSION"

echo ""
echo "✅ Release v$NEW_VERSION is ready!"
echo ""
echo "To publish the release:"
echo "  git push origin main"
echo "  git push origin v$NEW_VERSION"
echo ""
echo "Then create a GitHub release at:"
echo "  https://github.com/saravanasai/goqueue/releases/new?tag=v$NEW_VERSION"
