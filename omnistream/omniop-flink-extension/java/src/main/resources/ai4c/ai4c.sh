#!/usr/bin/bash
source /etc/profile
cd "$(dirname "$0")" && make clean && make -j16
