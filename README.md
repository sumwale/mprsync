mprsync or "multiprocess rsync" runs multiple rsync processes concurrently to sync data
much faster over slower and/or high latency networks (e.g. backups across continents)

There are two versions provided: a bash script `mprsync.sh` and a python module `mprsync.sync`
(latter is invoked by the wrapper `mprsync` script installed by `pip/pipx`).


## Features

- fetches affected files/directories in first pass with their sizes to distribute evenly among the
  parallel rsync processes
- curates the rsync options obtained before passing to the first step to skip any
  non-desirable output, so you can use all of rsync options that will only apply to
  subsequent steps
- curates the source locations for the cases of trailing slash vs without slash, with
  or without `-R/--relative` arguments to work correctly in all cases for the fetch path
  list of the first step (sets it apart from other similar utilities out there
  that falter with different combinations of source/destination specifications)
- correctly takes care of deletes and directory metadata changes (if asked in rsync flags)
  unlike other similar utilities
- no dependencies other than bash, rsync and standard POSIX utilities like sed, awk
- python version of the utility (in module `mprsync.sync`) depends on only python >= 3.10 and
  in addition to above, also provides:
  * parallelism between the fetch path phase and the fetch data phase so that the path list
    obtained is pipelined to the rsync jobs on-the-fly taking into account the path sizes sent to
    each job so far (e.g. prioritize the job with smallest total size so far)
  * take care of grouping paths together into `chunk-size` (as per their fetched sizes) so
    that small files (probably in the same directory) do not get strewn across multiple jobs
  * cycles chunks other threads if the selected thread is slow to empty its queue
  * up to 2 retries (i.e. total of 3 tries) for each thread rsync job in case it fails due to
    an unexpected reason; permission errors and the like do not count for "unexpected" which
    is determined using a combination of the rsync exit code and it standard error messages

Due to above additional features, the python version should be faster over slow networks when the
number of paths is quite large and the fetch phase ends of taking a significant proportion of the
total time. For most other cases, the performance of the two will be similar.


## Installation

To install the bash version, just download and copy the
[mprsync.sh](https://github.com/sumwale/mprsync/blob/main/mprsync.sh?raw=true) script somewhere
in your `$PATH`. In current Linux distributions a good place is `~/.local/bin` which should be in
`$PATH`. Then provide execute permission to the script (`chmod +x mprsync.sh`).

To install the python version, you can use `pip` (or `pipx` in newer distributions):

```sh
pip/pipx install mprsync
```

You can then run the python version as `mprsync` (assuming `~/.local/bin` is in `$PATH` which is
where `pip/pipx` will normally put the wrapper executable). Note that in newer releases,
installation using `pip` requires doing so in a virtual environment (which `pipx` handles
automatically) unless the flag `--break-system-packages` is used. Since this module depends on
nothing apart from python >= 3.10, you can safely use that flag with `pip`.

Alternatively you can skip all this and just download the
[sync.py](https://github.com/sumwale/mprsync/blob/main/mprsync/sync.py?raw=true) file, then run
it using `python/python3`: `python3 sync.py --jobs=10 <rsync args ...>`


## Usage

None of the additional options added by `mprsync/mprsync.sh` (apart from `-h/--help`) conflict
with rsync options, so you can just mix match the them with any required rsync options.
The `-h/--help` option details the additional options:

(for bash script)
```
Usage: mprsync.sh [-j JOBS|--jobs=JOBS] [--ignore-fetch-errors] [--silent] [-h|--help]
       <rsync options> SRC... DEST

Run multiple rsync processes to copy local/remote files and directories

Arguments:
  SRC...                 the source location(s) for rsync
  DEST                   the destination location for rsync

Options:
  -j, --jobs=JOBS        number of parallel jobs to use (default: 8)
  --ignore-fetch-errors  ignore permission or any other errors in the fetch path name
                         phase to continue to fetch data phase
  --silent               don't print any informational messages from mprsync.sh
  -h, --help             show this help message and exit
```

(for python script)
```
usage: mprsync [-h] [-j JOBS] [--chunk-size CHUNK_SIZE] [--silent]

Run multiple rsync processes to copy local/remote files and directories

options:
  -h, --help            show this help message and exit
  -j JOBS, --jobs JOBS  number of parallel jobs to use
  --chunk-size CHUNK_SIZE
                        minimum chunk size (in bytes) for splitting paths among the jobs
  --silent              don't print any informational messages from this program (does not
                        affect rsync output which is governed by its own flags)
```

The additional options need not be placed at the start before rsync ones, but it might
be clearer to do so. For example to run 10 parallel rsync jobs to sync data from a
remote server to local:

```sh
mprsync.sh --jobs=10 -aH --info=progress <user>@<server>:/data/ data/
```

```sh
mprsync -j 10 --chunk-size=16777216 -aH --zc=zstd --zl=1 --info=progress2 <user>@<server>:/data/ data/
```

### CPU usage with compression

When using compression, in some cases the CPU on the server or client can turn
into a bottleneck due to multiple concurrent rsync processes. For instance, running
10 jobs with gzip compression (`-z` option to rsync) on an entry-level online storage
box or a basic online VPS storage can easily run into severe CPU bottlenecks on the
storage nodes especially for downloads requiring compression on the server.

If you are not getting expected benefits in download/upload performance compared to
plain rsync, then this may be a cause. Hence it may be better to reduce the compression
level when running `mprsync/mprsync.sh` compared to the usual rsync usage and/or reduce the
number of jobs.

Recent versions of rsync allow for using `zstd` or `lz4` algorithms that are much
lighter on CPU and can provide similar level of compression as gzip. Monitor the
client and server CPU usage, if possible, and start with `--zc=zstd --zl=1` which is
light enough even for modern single/dual core VPS boxes while providing good amount
of compression. Using `lz4` will be fastest (at its default of compression level 1)
but provides the least amount of compression. Comparitively `zstd` level 1 is a bit
more expensive than `lz4` level 1 but has much higher compression. If you need to reduce
bandwidth usage and want to keep higher compression levels, then it is still better to
use `zstd` levels 3-6 that usually give better compression than default gzip (`-z`)
with much lower CPU usage, and then reduce the number of parallel jobs.
