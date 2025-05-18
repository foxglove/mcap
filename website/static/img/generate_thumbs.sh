#!/bin/bash
# This script generates the smaller png and webp thumbnails from mcap.svg
# Do not manually edit the generated thumbnails!
set -eux

cd "$(dirname "$0")"
magick -background none mcap.svg -resize 36x36 -gravity center -extent 32x32 -sharpen 0x1.0 favicon.png
magick -background none mcap.svg -resize 240x -sharpen 0x0.5 mcap240.webp
magick -background none mcap.svg -resize 720x -sharpen 0x0.5 mcap720.webp
ls -lh
