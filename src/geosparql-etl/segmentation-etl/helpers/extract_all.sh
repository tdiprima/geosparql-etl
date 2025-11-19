#!/bin/bash
# Parallelize extraction

find . -type f -name "*.tar.gz" | parallel 'echo "Extracting {}" && tar -xzf {} -C $(dirname {}) && rm -f {}'
