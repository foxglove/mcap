#!/usr/bin/env bash

#############################################################################
#
# This scripts queries and modifies CPU scaling frequencies to produce more
# accurate benchmark results. To move from a low power state to a higher
# one, run 'governor.sh performance'. To move back to a low power state
# run 'governor.sh powersave' or 'governor.sh ondemand' or reboot.
#
# Written and placed in public domain by Jeffrey Walton. The script based on
# code by Andy Polyakov, http://www.openssl.org/~appro/cryptogams/.
#
# Crypto++ Library is copyrighted as a compilation and (as of version 5.6.2)
# licensed under the Boost Software License 1.0, while the individual files
# in the compilation are all public domain.
#
# See https://www.cryptopp.com/wiki/Benchmarks for more details
#
#############################################################################

# Fixup ancient Bash
# https://unix.stackexchange.com/q/468579/56041
if [[ -z "${BASH_SOURCE[0]}" ]]; then
	BASH_SOURCE="$0"
fi

if [[ "$EUID" -ne 0 ]]; then
    echo "This script must be run as root"
    [[ "$0" = "${BASH_SOURCE[0]}" ]] && exit 1 || return 1
fi

if [ "x$1" = "x" ]; then
    echo "usage: $0 on[demand]|pe[rformance]|po[wersave]|us[erspace]?"
    [[ "$0" = "${BASH_SOURCE[0]}" ]] && exit 1 || return 1
fi

# "on demand" may result in a "invalid write argument" or similar
case $1 in
    on*|de*)    governor="ondemand";;
    po*|pw*)    governor="powersave";;
    pe*)        governor="performance";;
    co*)        governor="conservative";;
    us*)        governor="userspace";;
    \?)         ;;
    *)          echo "$1: unrecognized governor";;
esac

if [ -z "$governor" ]; then
	[[ "$0" = "${BASH_SOURCE[0]}" ]] && exit 1 || return 1
fi

cpus=$(ls /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null)

if [ -z "$cpus" ]; then
	echo "Failed to read CPU system device tree"
	[[ "$0" = "${BASH_SOURCE[0]}" ]] && exit 1 || return 1
fi

echo "Current CPU governor scaling settings:"
count=0
for cpu in $cpus; do
	echo "  CPU $count: $(cat "$cpu")"
	((count++))
done

if [ "x$governor" != "x" ]; then
    for cpu in $cpus; do
        echo "$governor" > "$cpu"
    done
fi

echo "New CPU governor scaling settings:"
count=0
for cpu in $cpus; do
	echo "  CPU $count: $(cat "$cpu")"
	((count++))
done

[[ "$0" = "${BASH_SOURCE[0]}" ]] && exit 0 || return 0
