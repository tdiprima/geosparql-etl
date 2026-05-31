#!/usr/bin/env bash
# Extracts all .tar.gz archives found under the current directory in parallel.
# Removes each archive only after a successful extraction.

set -euo pipefail

find . -type f -name "*.tar.gz" \
    | parallel 'echo "Extracting {}" \
                && tar --extract --gzip --file {} --directory "$(dirname {})" \
                && rm --force {}'
