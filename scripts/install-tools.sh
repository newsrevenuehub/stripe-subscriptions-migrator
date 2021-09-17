#!/usr/bin/env bash

set -o nounset
set -o errexit
set -o pipefail
#set -o xtrace

echo 'debconf debconf/frontend select Noninteractive' | debconf-set-selections
apt-get -qq update
apt-get -qq install vim less ripgrep dos2unix > /dev/null
echo "set -o vi" >> /etc/bash.bashrc
