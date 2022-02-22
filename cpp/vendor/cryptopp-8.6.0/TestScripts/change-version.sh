#!/usr/bin/env bash

#############################################################################
#
# This script switches back to the previous Crypto++ version before
# building the docs. Before running the script, copy it to the root
# directory. After running this script, you can 'make docs'
#
# Written and placed in public domain by Jeffrey Walton.
#
# Crypto++ Library is copyrighted as a compilation and (as of version 5.6.2)
# licensed under the Boost Software License 1.0, while the individual files
# in the compilation are all public domain.
#
# See https://www.cryptopp.com/wiki/Release_Versioning for more details
#
#############################################################################

sed 's/Library 8.6 API/Library 8.5 API/g' cryptlib.h > cryptlib.h.new
mv cryptlib.h.new cryptlib.h

sed 's/= 8.6/= 8.5/g' Doxyfile > Doxyfile.new
mv Doxyfile.new Doxyfile

sed 's/CRYPTOPP_MINOR 6/CRYPTOPP_MINOR 5/g' config_ver.h > config_ver.h.new
mv config_ver.h.new config_ver.h

sed 's/CRYPTOPP_VERSION 860/CRYPTOPP_VERSION 850/g' config_ver.h > config_ver.h.new
mv config_ver.h.new config_ver.h
