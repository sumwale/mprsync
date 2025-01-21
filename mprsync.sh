#!/bin/bash

# script to run rsync in parallel which gives it quite a large boost over long distance
# network or even locally when a large number of files have to be synchronized between
# a source and a destination

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
  echo -e $fg_green
  echo "Usage: $SCRIPT [-j JOBS|--jobs=JOBS] [--skip-full-rsync] [--silent] [--usage]"
  echo "       <rsync options> SRC... DEST"
  echo
  echo "Run multiple rsync processes to copy local/remote files"
  echo
  echo "Arguments:"
  echo "  SRC...             the source location(s) for rsync"
  echo "  DEST               the destination location for rsync"
  echo
  echo "Options:"
  echo "  -j, --jobs=JOBS    number of parallel jobs to use (default: $def_num_jobs)"
  echo "  --skip-full-rsync  skip the full rsync at the end -- use only if no directory"
  echo "                     metadata changes, or file/directory deletes are required"
  echo "  --silent           don't print any informational messages"
  echo "  --usage            show this help message and exit"
  echo -e $fg_reset
}

num_jobs=$def_num_jobs
skip_full_rsync=
is_relative=
# arrays allow dealing with spaces and special characters
declare -a rsync_opts
declare -a l_rsync_opts # trimmed options used for listing files to be added/updated
declare -a rsync_args

while [ -n "$1" ]; do
  case "$1" in
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
    --skip-full-rsync)
      skip_full_rsync=1
      shift
      ;;
    --silent)
      silent=1
      shift
      ;;
    --usage)
      usage
      exit 0
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

# get the full file list and split into given number of jobs

file_prefix=$(mktemp)

trap "/bin/rm -f $file_prefix*" 0 1 2 3 4 5 6 11 12 15

# remove options like --info, --debug etc from file list fetch call and negate verbosity
for opt in "${rsync_opts[@]}"; do
  if ! [[ "$opt" =~ ^--info=|^--debug=|^--progress$ ]]; then
    l_rsync_opts+=("$opt")
    if [[ "$opt" =~ ^-[^-]*R|^--relative$ ]]; then
      is_relative=1
    fi
  fi
done
sep=//// # use a separator that cannot appear in file paths
# keep only the files to be added/updated in the parallel runs
rsync "${l_rsync_opts[@]}" --no-v --dry-run --out-format="%l$sep%n" "${rsync_args[@]}" | \
  { grep -Ev '^deleting |^created |^skipping |^sending incremental |/$' || true; } >> $file_prefix

if [ $(wc -c $file_prefix | cut -d' ' -f1) -le 1 ]; then
  if [ -z "$silent" ]; then
    echo -e "${fg_orange}No data to transfer. Will continue with metadata sync and any deletes.$fg_reset"
  fi
  rsync "${rsync_opts[@]}" "${rsync_args[@]}"
  exit $?
fi

# Total the file sizes, divide by number of parallel jobs and distribute files in
# round robin to the jobs until the total exceeds the required for the jobs.
# Note: round robin also allows avoiding the allocating too many small files to a single
# job in case a directory has tons of them.

AWK=awk
type -p mawk >/dev/null && AWK=mawk
total_size=$($AWK -F $sep '{ sum += $1 } END { print sum }' $file_prefix)
total_psize=$(( total_size / num_jobs ))

if [ -z "$silent" ]; then
  readable_mb=$(bc -l <<< "scale=2; $total_size/(1024 * 1024)")
  echo -e "${fg_green}Splitting files having $readable_mb MB of data into $num_jobs jobs$fg_reset"
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

# Adjust source paths as per expected file list received above which is that if there
# is no trailing slash, then file list contains the last element hence remove it.
# When -R/--relative is being used then path names provided are full names hence
# trim the paths in the source till root "/" or "."
orig_rsync_args=("${rsync_args[@]}")
nargs_1=$(( ${#rsync_args[@]} - 1 ))
for idx in "${!rsync_args[@]}"; do
  # skip any processing for the destination
  if [ $idx -ne $nargs_1 ]; then
    if [ -n "$is_relative" ]; then
      # for this case the file names already have the full paths,
      # so the source should be trimmed all the way till the root (i.e. "/" or ".")
      rsync_args[$idx]=$(echo "${rsync_args[$idx]}" | \
        sed -E 's#(^|:)([/.]).*#\1\2#;s#(^|:)[^/.:][^:]*$#\1.#')
    elif [[ "${rsync_args[$idx]}" != */ ]]; then
      rsync_args[$idx]="${rsync_args[$idx]%*/*}"
    fi
  fi
done

if [ -z "$silent" ]; then
  echo -e "${fg_green}Running $num_jobs parallel rsync jobs...$fg_reset"
  echo
fi

for split_file in $(echo $file_prefix.*); do
  rsync --files-from=$split_file "${rsync_opts[@]}" "${rsync_args[@]}" &
  if [ $num_jobs -gt 8 ]; then
    # forking rsync too quickly sometimes causes trouble
    sleep 0.3
  fi
done

wait

if [ -z "$skip_full_rsync" ]; then
  # run a final rsync to perform deletions or any metadata changes
  if [ -z "$silent" ]; then
    echo -e "${fg_green}Running final rsync for metadata, deletions and remaining changes$fg_reset"
    echo
  fi
  rsync "${rsync_opts[@]}" "${orig_rsync_args[@]}"
fi
