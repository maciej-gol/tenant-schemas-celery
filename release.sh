#!/bin/bash
set -euo pipefail

# Release script for tenant-schemas-celery.
# Tags the current version, builds the sdist+wheel, and prints a link
# to create the matching GitHub release. Optionally uploads to PyPI.
#
# Usage:
#   ./release.sh            # interactive: confirms before pushing the tag
#   ./release.sh --yes      # skip the push-tag confirmation
#   ./release.sh --upload   # also run `twine upload dist/*`
#   Flags can be combined:   ./release.sh --yes --upload
#
# If the tag already exists it will ask before overwriting it (force-push).
# NOTE: overwriting a tag requires --yes (or answering y) at the prompt.
#
# Prerequisites:
#   - python, python -m build (or setuptools) on PATH
#   - twine on PATH (only for --upload)
#   - git remote "origin" pointing at the GitHub repository

VERSION_FILE="VERSION"
REMOTE="origin"
FORCE_YES=""
DO_UPLOAD=""

info()  { printf '\033[1;34m[release]\033[0m %s\n' "$*"; }
die()   { printf '\033[1;31m[release] error:\033[0m %s\n' "$*" >&2; exit 1; }

for arg in "$@"; do
    case "$arg" in
        --yes)   FORCE_YES="--yes" ;;
        --upload) DO_UPLOAD="--upload" ;;
        *) die "unknown argument: $arg" ;;
    esac
done

# ---------------------------------------------------------------------------
# 1. Sanity checks
# ---------------------------------------------------------------------------
[[ -f "$VERSION_FILE" ]] || die "$VERSION_FILE not found; run me from the repo root"

VERSION="$(tr -d '[:space:]' < "$VERSION_FILE")"
[[ -n "$VERSION" ]] || die "empty version in $VERSION_FILE"
[[ "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "unexpected version format: '$VERSION'"

BRANCH="$(git symbolic-ref --short HEAD 2>/dev/null || true)"
if [[ "$BRANCH" != "master" ]]; then
    die "not on master (on '$BRANCH'); run from master"
fi

if [[ -n "$(git status --porcelain)" ]]; then
    die "working tree is dirty; commit or stash before releasing"
fi

# ---------------------------------------------------------------------------
# 2. Resolve GitHub owner/repo from the remote URL (https:// or git@/ssh)
# ---------------------------------------------------------------------------
REMOTE_URL="$(git config --get "remote.$REMOTE.url" || die "no remote '$REMOTE' configured")"
OWNER_REPO="$(sed -E 's#(git@|https://)[^:/]+[:/]##; s#\.git$##' <<< "$REMOTE_URL")"
[[ "$OWNER_REPO" =~ ^[^/]+/[^/]+$ ]] || die "could not parse owner/repo from '$REMOTE_URL'"
RELEASE_URL="https://github.com/$OWNER_REPO/releases/new?tag=$VERSION"

# ---------------------------------------------------------------------------
# 3. Check whether the tag already exists; decide on overwrite
# ---------------------------------------------------------------------------
OVERWRITE=""
if git rev-parse -q --verify "refs/tags/$VERSION" >/dev/null; then
    info "tag '$VERSION' already exists locally"
    read -r -p "[release] overwrite tag '$VERSION' (force-push)? [y/N] " -n 1 REPLY
    echo
    if [[ "$REPLY" =~ ^[Yy]$ ]]; then
        OVERWRITE="--force"
    else
        die "aborted; tag '$VERSION' already exists"
    fi
fi

TAG_NOTE=""
[[ -n "$OVERWRITE" ]] && TAG_NOTE=" (force-overwrite)"
info "version:      $VERSION"
info "branch:       $BRANCH"
info "remote:       $OWNER_REPO"
info "will tag:     $VERSION$TAG_NOTE"
info "will build:   sdist + wheel"
info "will upload:  $([ "$DO_UPLOAD" == "--upload" ] && echo "yes (twine)" || echo "no")"

# ---------------------------------------------------------------------------
# 4. Create and (optionally) push the tag
# ---------------------------------------------------------------------------
if [[ -n "$OVERWRITE" ]]; then
    git tag -f "$VERSION"
else
    git tag "$VERSION"
fi

if [[ "$FORCE_YES" != "--yes" ]]; then
    read -r -p "[release] push tag '$VERSION' to origin? [y/N] " -n 1 REPLY
    echo
    if [[ ! "$REPLY" =~ ^[Yy]$ ]]; then
        info "tag '$VERSION' created locally but NOT pushed (use: git push $REMOTE $VERSION)"
    else
        FORCE_YES="--yes"
    fi
fi

if [[ "$FORCE_YES" == "--yes" ]]; then
    if [[ -n "$OVERWRITE" ]]; then
        git push --force "$REMOTE" "$VERSION"
        info "tag '$VERSION' force-pushed (overwritten)"
    else
        git push "$REMOTE" "$VERSION"
        info "tag '$VERSION' pushed"
    fi
fi

# ---------------------------------------------------------------------------
# 5. Build the distribution files
# ---------------------------------------------------------------------------
rm -rf dist build *.egg-info

if python -m build --version >/dev/null 2>&1; then
    python -m build
elif python -c "import setuptools" >/dev/null 2>&1; then
    info "python -m build not available; falling back to setup.py"
    python setup.py sdist bdist_wheel
else
    die "neither 'build' nor 'setuptools' installed"
fi

ls -1 dist/*.{whl,tar.gz} 2>/dev/null \
    || die "expected sdist/wheel not produced in dist/"

# ---------------------------------------------------------------------------
# 6. Optionally upload to PyPI
# ---------------------------------------------------------------------------
if [[ "$DO_UPLOAD" == "--upload" ]]; then
    command -v twine >/dev/null 2>&1 || die "twine not found on PATH (required for --upload)"
    info "uploading to PyPI with twine..."
    twine upload dist/*
    info "upload complete"
fi

# ---------------------------------------------------------------------------
# 7. Print the release link
# ---------------------------------------------------------------------------
echo
info "build artifacts in: $(pwd)/dist"
info "create the GitHub release here:"
printf '\033[1;36m%s\033[0m\n' "$RELEASE_URL"
