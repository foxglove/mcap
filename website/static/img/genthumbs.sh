#!/bin/bash
set -eux

magick -background none mcap.svg -resize 36x36 -gravity center -extent 32x32 -sharpen 0x1.0 favicon.png
magick -background none mcap.svg -resize 240x mcap240.webp
magick -background none mcap.svg -resize 720x mcap720.webp
ls -lh
