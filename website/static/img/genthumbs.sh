#!/bin/bash
set -eux

magick mcap.svg -resize 240x mcap240.webp
magick mcap.svg -resize 720x mcap720.webp
ls -lh *.svg *.webp
