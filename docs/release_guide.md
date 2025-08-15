# Release Management Guide

This guide explains how to handle versioning, releases, and fixes for GoQueue.

## Version Format

GoQueue follows [Semantic Versioning](https://semver.org/) (SemVer):

- `MAJOR.MINOR.PATCH` (e.g., `1.2.3`)
- **MAJOR**: Breaking changes
- **MINOR**: New features, backward compatible
- **PATCH**: Bug fixes, backward compatible

## Current Version: 0.0.1

Starting with `0.0.1` to gather user feedback before the stable 1.0.0 release.

## Release Process

### 1. Planning a Release

Before releasing, ensure:

- [ ] All tests pass: `go test ./...`
- [ ] Code is linted: `golangci-lint run`
- [ ] Documentation is updated
- [ ] CHANGELOG is updated

### 2. Version Update Steps

#### For Patch Release (Bug fixes: 0.0.1 → 0.0.2)

```bash
# 1. Update version in version.go
sed -i 's/const Version = "0.0.1"/const Version = "0.0.2"/' version.go

# 2. Update README.md badge
sed -i 's/Version-0.0.1-green/Version-0.0.2-green/' README.md

# 3. Commit changes
git add version.go README.md
git commit -m "Release v0.0.2"

# 4. Create and push tag
git tag v0.0.2
git push origin main
git push origin v0.0.2
```

#### For Minor Release (New features: 0.0.1 → 0.1.0)

```bash
# 1. Update version
sed -i 's/const Version = "0.0.1"/const Version = "0.1.0"/' version.go
sed -i 's/Version-0.0.1-green/Version-0.1.0-green/' README.md

# 2. Commit and tag
git add version.go README.md
git commit -m "Release v0.1.0: Add new features"
git tag v0.1.0
git push origin main
git push origin v0.1.0
```

#### For Major Release (Breaking changes: 0.1.0 → 1.0.0)

```bash
# 1. Update version
sed -i 's/const Version = "0.1.0"/const Version = "1.0.0"/' version.go
sed -i 's/Version-0.1.0-green/Version-1.0.0-green/' README.md

# 2. Commit and tag
git add version.go README.md
git commit -m "Release v1.0.0: Stable release"
git tag v1.0.0
git push origin main
git push origin v1.0.0
```

### 3. Automated Release Script

Create `scripts/release.sh`:

```bash
#!/bin/bash
set -e

if [ $# -ne 1 ]; then
    echo "Usage: $0 <version>"
    echo "Example: $0 0.0.2"
    exit 1
fi

NEW_VERSION=$1
CURRENT_VERSION=$(grep 'const Version' version.go | cut -d'"' -f2)

echo "Updating version from $CURRENT_VERSION to $NEW_VERSION"

# Update version.go
sed -i "s/const Version = \"$CURRENT_VERSION\"/const Version = \"$NEW_VERSION\"/" version.go

# Update README.md
sed -i "s/Version-$CURRENT_VERSION-green/Version-$NEW_VERSION-green/" README.md

# Run tests
echo "Running tests..."
go test ./...

# Run linter
echo "Running linter..."
golangci-lint run

# Commit changes
git add version.go README.md
git commit -m "Release v$NEW_VERSION"

# Create tag
git tag "v$NEW_VERSION"

echo "✅ Release v$NEW_VERSION ready!"
echo "To publish: git push origin main && git push origin v$NEW_VERSION"
```

Make it executable:

```bash
chmod +x scripts/release.sh
```

## Hotfix Process

For urgent bug fixes that need immediate release:

```bash
# 1. Create hotfix branch from main
git checkout main
git pull origin main
git checkout -b hotfix/critical-bug-fix

# 2. Make the fix
# ... edit files ...

# 3. Test the fix
go test ./...
golangci-lint run

# 4. Commit fix
git add .
git commit -m "Fix critical bug in job processing"

# 5. Merge to main
git checkout main
git merge hotfix/critical-bug-fix

# 6. Release patch version
./scripts/release.sh 0.0.2

# 7. Push
git push origin main
git push origin v0.0.2

# 8. Clean up
git branch -d hotfix/critical-bug-fix
```

## GitHub Releases

After pushing tags, create GitHub releases:

1. Go to `https://github.com/saravanasai/goqueue/releases`
2. Click "Create a new release"
3. Choose the tag (e.g., `v0.0.2`)
4. Write release notes:

````markdown
## Changes

- Fixed critical bug in job processing
- Improved error handling in Redis adapter

## Installation

```bash
go get github.com/saravanasai/goqueue@v0.0.2
```
````

````

## Version Checking in Code

Users can check the version programmatically:

```go
package main

import (
    "fmt"
    "github.com/saravanasai/goqueue"
)

func main() {
    fmt.Printf("GoQueue version: %s\n", goqueue.GetVersion())
}
````

## Best Practices

1. **Test Before Release**: Always run full test suite
2. **Update Documentation**: Keep README.md and docs updated
3. **Semantic Versioning**: Follow SemVer strictly
4. **Change Logs**: Maintain CHANGELOG.md for user-facing changes
5. **Backward Compatibility**: Avoid breaking changes in minor/patch releases
6. **Security Fixes**: Release security patches immediately

## Pre-1.0.0 Guidelines

While in 0.x.x versions:

- Gather user feedback actively
- Fix bugs quickly (patch releases)
- Add features based on feedback (minor releases)
- Breaking changes are acceptable but should be documented
- Focus on API stability as you approach 1.0.0

## 1.0.0 Criteria

Before releasing 1.0.0, ensure:

- [ ] API is stable and well-documented
- [ ] All major features are implemented
- [ ] Comprehensive test coverage
- [ ] Production-ready performance
- [ ] Security review completed
- [ ] User feedback incorporated
