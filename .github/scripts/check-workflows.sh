#!/usr/bin/env bash
set -euo pipefail
# Official release checksum; update together with the pinned version.
tool_dir=$(mktemp -d)
trap 'rm -rf "$tool_dir"' EXIT
archive=actionlint_1.7.12_linux_amd64.tar.gz
curl --fail --silent --show-error --location --retry 3 \
  "https://github.com/rhysd/actionlint/releases/download/v1.7.12/$archive" -o "$tool_dir/$archive"
(cd "$tool_dir" && echo "8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8  $archive" | sha256sum --check)
tar -xzf "$tool_dir/$archive" -C "$tool_dir" actionlint
"$tool_dir/actionlint" .github/workflows/*.yml
