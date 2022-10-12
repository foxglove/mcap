#!/usr/bin/env bash
# Used in go CI on tagged workflows.
# Checks that the current commit is tagged with the correct MCAP library version.
set -eo pipefail

if [ $# -ne 1 ]; then
    echo "Usage: $0 <path-to-mcap-binary>"
    exit 1
fi

expected_tag="go/mcap/$($1 version --library)"
read -ra all_tags <<< "$(git tag --points-at HEAD)"
found="false"
for tag in "${all_tags[@]}"; do
    if [ "$tag" = "$expected_tag" ]; then
        found="true"
    fi
done

if [ "$found" != "true" ]; then
    echo "failed: expected tag $expected_tag in found tags: [${all_tags[*]}]"
    exit 1
else
    echo "success"
fi
