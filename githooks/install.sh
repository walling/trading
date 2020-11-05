#!/bin/bash

# Immediately exit on failure
set -e

# Go to .git hooks directory
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR/../.git/hooks"

install_githook() {
  filename="$1"
  target="../../githooks/$filename"
  if [[ -e "$filename" ]]; then
    if [[ "$(readlink $filename)" == "$target" ]]; then
      echo "already installed: $filename"
    else
      echo "warning: not replacing differing hook script: $filename"
    fi
  else
    echo "installing: $filename"
    ln -s "$target" .
  fi
}

# Install hooks, ask to replace
echo "installing git hooks ..."
install_githook "pre-commit"
echo "done"
