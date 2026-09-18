#!/usr/bin/env bash
# Project:   dfe-transform-vrl
# File:      scripts/fetch-helm.sh
# Purpose:   Download and cache a pinned helm binary for the chart render tests
# Language:  Bash
#
# License:   BUSL-1.1
# Copyright: (c) 2026 HYPERI PTY LIMITED
#
# Usage:
#   scripts/fetch-helm.sh
#   HELM_VERSION=v4.2.4 scripts/fetch-helm.sh
#
# The KEDA trigger test renders the committed chart with a real `helm
# template`, and the ARC runners carry no helm, so the test fetches one
# rather than skipping the render.
#
# Environment variables (all optional):
#   HELM_VERSION   Release tag to fetch (default below)
#
# Behaviour:
#   - Resolves the release archive for this OS and CPU architecture
#   - Verifies the published SHA-256 before the archive is unpacked
#   - Caches under .tmp/helm/<version>-<os>-<arch>/, which is gitignored
#   - Re-running reuses the cache, so it is safe to call per test run
#   - Prints the absolute path to the helm binary as the last line of stdout

set -euo pipefail

# Pinned so a helm release cannot change what CI renders overnight; override
# with HELM_VERSION to test against a newer one.
HELM_VERSION="${HELM_VERSION:-v4.3.0}"
BASE_URL="https://get.helm.sh"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

# ----------------------------------------------------------------------------
# Platform
# ----------------------------------------------------------------------------

case "$(uname -s)" in
    Linux) OS="linux" ;;
    Darwin) OS="darwin" ;;
    *)
        echo "fetch-helm: unsupported OS $(uname -s)" >&2
        exit 1
        ;;
esac

case "$(uname -m)" in
    x86_64 | amd64) ARCH="amd64" ;;
    aarch64 | arm64) ARCH="arm64" ;;
    *)
        echo "fetch-helm: unsupported architecture $(uname -m)" >&2
        exit 1
        ;;
esac

CACHE_DIR="$REPO_ROOT/.tmp/helm/$HELM_VERSION-$OS-$ARCH"
HELM_BIN="$CACHE_DIR/helm"

# ----------------------------------------------------------------------------
# Cache hit
# ----------------------------------------------------------------------------

if [[ -x "$HELM_BIN" ]] && "$HELM_BIN" version >/dev/null 2>&1; then
    echo "fetch-helm: cached $HELM_VERSION at $HELM_BIN" >&2
    echo "$HELM_BIN"
    exit 0
fi

# ----------------------------------------------------------------------------
# Download
# ----------------------------------------------------------------------------

TARBALL="helm-$HELM_VERSION-$OS-$ARCH.tar.gz"

# Staged in a sibling of the cache dir so the final move stays on one
# filesystem and a half-written binary is never left where a test can run it.
mkdir -p "$CACHE_DIR"
WORK_DIR="$(mktemp -d "$CACHE_DIR/.fetch-XXXXXX")"

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT INT TERM

echo "fetch-helm: downloading $BASE_URL/$TARBALL" >&2
curl -fsSL --retry 3 --retry-delay 2 -o "$WORK_DIR/$TARBALL" "$BASE_URL/$TARBALL"
curl -fsSL --retry 3 --retry-delay 2 -o "$WORK_DIR/$TARBALL.sha256sum" \
    "$BASE_URL/$TARBALL.sha256sum"

# ----------------------------------------------------------------------------
# Verify before the archive is unpacked
# ----------------------------------------------------------------------------

if command -v sha256sum >/dev/null 2>&1; then
    ACTUAL_SUM="$(sha256sum "$WORK_DIR/$TARBALL" | cut -d' ' -f1)"
elif command -v shasum >/dev/null 2>&1; then
    ACTUAL_SUM="$(shasum -a 256 "$WORK_DIR/$TARBALL" | cut -d' ' -f1)"
else
    echo "fetch-helm: no sha256sum or shasum available to verify the download" >&2
    exit 1
fi

EXPECTED_SUM="$(cut -d' ' -f1 <"$WORK_DIR/$TARBALL.sha256sum")"

if [[ -z "$EXPECTED_SUM" || "$ACTUAL_SUM" != "$EXPECTED_SUM" ]]; then
    echo "fetch-helm: checksum mismatch for $TARBALL" >&2
    echo "  expected: ${EXPECTED_SUM:-<empty>}" >&2
    echo "  actual:   $ACTUAL_SUM" >&2
    exit 1
fi

echo "fetch-helm: sha256 verified" >&2

# ----------------------------------------------------------------------------
# Unpack and install
# ----------------------------------------------------------------------------

tar -xzf "$WORK_DIR/$TARBALL" -C "$WORK_DIR"

EXTRACTED="$WORK_DIR/$OS-$ARCH/helm"
if [[ ! -f "$EXTRACTED" ]]; then
    echo "fetch-helm: $TARBALL holds no $OS-$ARCH/helm" >&2
    exit 1
fi

chmod +x "$EXTRACTED"
mv -f "$EXTRACTED" "$HELM_BIN"

if ! "$HELM_BIN" version >/dev/null 2>&1; then
    echo "fetch-helm: $HELM_BIN does not run on this host" >&2
    exit 1
fi

echo "fetch-helm: installed $HELM_VERSION at $HELM_BIN" >&2
echo "$HELM_BIN"
