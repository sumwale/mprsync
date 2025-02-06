#!/bin/bash

# script to run rsync in parallel which gives it quite a large boost over long distance
# or slow networks that may remain underutilized with just a single rsync

set -e
set -o pipefail

# ensure that system path is always searched first for all the utilities
export PATH="/usr/sbin:/usr/bin:/sbin:/bin:/usr/local/sbin:/usr/local/bin:$PATH"

SCRIPT="$(basename "${BASH_SOURCE[0]}")"

fg_red='\033[31m'
fg_green='\033[32m'
fg_orange='\033[33m'
fg_reset='\033[00m'

def_num_jobs=8

function usage() {
  echo
  echo "Usage: $SCRIPT [-h|--help] [-j JOBS|--jobs=JOBS] [--ignore-fetch-errors] [--silent]"
  echo "       <rsync options> SRC... DEST"
  echo
  echo "Run multiple rsync processes to copy local/remote files and directories"
  echo
  echo "Arguments:"
  echo "  SRC...                 the source location(s) for rsync"
  echo "  DEST                   the destination location for rsync"
  echo
  echo "Options:"
  echo "  -h, --help             show this help message and exit"
  echo "  -j, --jobs=JOBS        number of parallel jobs to use (default: $def_num_jobs)"
  echo "  --ignore-fetch-errors  ignore permission or any other errors in the fetch path name"
  echo "                         phase to continue to fetch data phase"
  echo "  --silent               don't print any informational messages from $SCRIPT (does not"
  echo "                         affect rsync output which is governed by its own flags)"
  echo
}

num_jobs=$def_num_jobs
ignore_fetch_errors=
is_relative=
# arrays allow dealing with spaces and special characters
declare -a rsync_opts
declare -a l_rsync_opts # trimmed options used for listing paths to be added/updated
declare -a rsync_args

while [ -n "$1" ]; do
  case "$1" in
    -h|--help)
      usage
      exit 0
      ;;
    -j|--jobs=*)
      if [ "$1" = "-j" ]; then
        num_jobs="$2"
        shift
      else
        num_jobs="${1/*=/}"
      fi
      if [ -z "$num_jobs" ]; then
        echo -e "${fg_red}missing value for option $1$fg_reset"
        usage
        exit 1
      fi
      if ! [ "$num_jobs" -gt 1 ] 2>/dev/null; then
        echo -e "$fg_red'$num_jobs' should be a number greater than 1 for option $1$fg_reset"
        usage
        exit 1
      fi
      shift
      ;;
    --ignore-fetch-errors)
      ignore_fetch_errors=1
      shift
      ;;
    --silent)
      silent=1
      shift
      ;;
    -*)
      rsync_opts+=("$1")
      shift
      ;;
    *)
      rsync_args+=("$1")
      shift
      ;;
  esac
done

# get the full path list and split into given number of jobs

file_prefix=$(mktemp)

trap "/bin/rm -f $file_prefix*" 0 1 2 3 4 5 6 11 12 15

# remove options like --info, --debug etc from path list fetch call and negate verbosity
for opt in "${rsync_opts[@]}"; do
  if ! [[ "$opt" =~ ^--info=|^--debug=|^--progress$ ]]; then
    l_rsync_opts+=("$opt")
    if [[ -z "$is_relative" && "$opt" =~ ^-[^-]*R|^--relative$ ]]; then
      is_relative=1
    fi
  fi
done
if [ -z "$ignore_fetch_errors" ]; then
  ignore_cmd=false
else
  ignore_cmd=true
fi

sep=//// # use a separator that cannot appear in paths

# use a fixed size of 1024 for deletes to ensure that they count towards some expense
{ rsync "${l_rsync_opts[@]}" --no-v --dry-run --out-format="%l$sep%n" "${rsync_args[@]}" || \
  eval $ignore_cmd; } | sed -n "s#^[0-9]\+#\0#p;s#^deleting #1024$sep#p" >> $file_prefix

if [ $(wc -c $file_prefix | cut -d' ' -f1) -le 1 ]; then
  if [ -z "$silent" ]; then
    echo -e "${fg_orange}No data to transfer.$fg_reset"
  fi
  exit $?
fi

# Total the path sizes, divide by number of parallel jobs and distribute paths to
# the jobs until their size allocation exceeds that limit.

AWK=awk
type -p mawk >/dev/null && AWK=mawk # mawk is faster than gawk and others
total_size=$($AWK -F $sep '{ sum += $1 } END { print sum }' $file_prefix)
total_psize=$(( total_size / num_jobs ))

if [ -z "$silent" ]; then
  readable_mb=$(bc -l <<< "scale=2; $total_size/(1024 * 1024)")
  echo -e "${fg_green}Splitting paths having $readable_mb MB of data into $num_jobs jobs$fg_reset"
fi

$AWK -F $sep -v num_jobs=$num_jobs -v total_psize=$total_psize -v file_prefix=$file_prefix '
BEGIN {
  idx = 0
} {
  # keep filling current job until it is full, then move to next
  print $2 >> file_prefix "." idx
  job_sizes[idx] += $1
  if (job_sizes[idx] >= total_psize) {
    idx = (idx + 1) % num_jobs
  }
}' < $file_prefix

# Adjust source paths as per expected path list received above which is that if there
# is no trailing slash, then path list contains the last element hence remove it.
# When -R/--relative is being used then path names provided are full names hence
# trim the paths in the source till root "/" or "."
orig_rsync_args=("${rsync_args[@]}")
nargs_1=$(( ${#rsync_args[@]} - 1 ))
for idx in "${!rsync_args[@]}"; do
  # skip any processing for the destination
  if [ $idx -ne $nargs_1 ]; then
    if [ -n "$is_relative" ]; then
      # for this case the paths already are the full paths,
      # so the source should be trimmed all the way till the root (i.e. "/" or ".")
      if [[ "${rsync_args[$idx]}" = rsync://* ]]; then
        rsync_args[$idx]=$(echo "${rsync_args[$idx]}" | \
          sed -E 's#(^|:)/.*#\1/#;s#(^|:)[^/:][^:]*$#\1.#')
      else
        rsync_args[$idx]=$(echo "${rsync_args[$idx]}" | \
          sed -E 's#^(rsync://[^/]*/)/.*#\1/#;s#^(rsync://[^/]*/)[^/].*$#\1.#')
      fi
    elif [[ "${rsync_args[$idx]}" != */ ]]; then
      # if the source does not end in a slash, then path list obtained above will
      # already contain the last directory of the path, hence remove it
      rsync_args[$idx]=$(echo "${rsync_args[$idx]}" | sed -E 's#(^|[/:])[^/:]*$#\1.#')
    fi
  fi
done

if [ -z "$silent" ]; then
  echo -e "${fg_green}Running $num_jobs parallel rsync jobs...$fg_reset"
  echo
fi

for split_file in $(echo $file_prefix.*); do
  rsync --files-from=$split_file "${rsync_opts[@]}" --no-r "${rsync_args[@]}" &
  # forking rsync+ssh too quickly sometimes causes trouble, so wait for sometime
  if [ $num_jobs -gt 8 ]; then
    sleep 0.3
  else
    sleep 0.1
  fi
done

wait
