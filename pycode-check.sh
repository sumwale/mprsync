#!/bin/bash

set -e

SCRIPT="$(basename "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

check_script=mprsync/sync.py
fg_green='\033[32m'
fg_reset='\033[00m'

{
  cd $SCRIPT_DIR
  echo -e "${fg_green}Running flake8 ...$fg_reset"
  flake8 $check_script
  echo -e "${fg_green}Running pyright ...$fg_reset"
  pyright $check_script
  echo -e "${fg_green}Running pylint ...$fg_reset"
  pylint $check_script;
}
