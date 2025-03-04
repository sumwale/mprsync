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
Usage: mprsync.sh [-h|--help] [-j JOBS|--jobs=JOBS] [--ignore-fetch-errors] [--silent]
       <rsync options> SRC... DEST

Run multiple rsync processes to copy local/remote files and directories

Arguments:
  SRC...                 the source location(s) for rsync
  DEST                   the destination location for rsync

Options:
  -h, --help             show this help message and exit
  -j, --jobs=JOBS        number of parallel jobs to use (default: 8)
  --ignore-fetch-errors  ignore permission or any other errors in the fetch path name
                         phase to continue to fetch data phase
  --silent               don't print any informational messages from mprsync.sh
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
  --full-rsync          run a full rsync at the end if any of the rsync processes failed
                        with unexpected errors
  --silent              don't print any informational messages from this program (does not
                        affect rsync output which is governed by its own flags)
```

The additional options need not be placed at the start before rsync ones, but it might
be clearer to do so. For example to run 10 parallel rsync jobs to sync data from a
remote server to local:

```sh
mprsync.sh --jobs=10 -aH --info=progress2 <user>@<server>:/data/ data/
```

Or using the python script with higher chunk size and with `zstd` compression level 1:


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

A note about SSH options: you might get better performance when using SSH transport using AES-GCM
ciphers when client and server support AES-NI acceleration (`grep -w aes /proc/cpuinfo`) like:
`mprsync ... --zc=zstd --zl=1 -e "ssh -o Compression=no -c aes256-gcm@openssh.com" ...`

### Some numbers

Here is a brief comparison of python mprsync vs mprsync.sh vs rsync on a 200Mbps link
pulling data from a backup from across continent (Europe to Asia). The local destination
is empty for all these runs.

This first one is around 13.1GB of data having 442K files most of which are small.

python mprsync:
```
> time python3 mprsync/sync.py -j 10 --info=progress2 --zc=zstd --zl=1 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" <remote source> <local destination>
Using rsync executable from /usr/bin/rsync
Running up to 10 parallel rsync jobs with paths split into 8388608 byte chunks (as per the sizes on source) ...
...
Executed in  385.86 secs    fish           external
```

mprsync.sh:
```
> time mprsync.sh -j 10 --ignore-fetch-errors --info=progress2 --zc=zstd --zl=1 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" <remote source> <local destination>
Fetching the list of paths to be updated and/or deleted ...
...
Executed in  456.80 secs    fish           external
```

rsync:
```
> time rsync --info=progress2 --zc=zstd --zl=8 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" <remote source> <local destination>
...
Executed in   17.47 mins    fish           external
```

For this case the python version is about 1.2X faster than the bash version and about 2.7X
faster than plain rsync. The 200Mbps link is more than 90% saturated for both the python
and bash scripts once the parallel rsync processes start (except at the tail end), while
it is less than 30% full with rsync for most of the run but it covers up some of the loss
due to higher compression and doing the sync in a single pass.


The second comparison is for video and picture data of around 13.1GB where the number of
files is only 743. The data is not compressible but the same flags are used as above
(since that is how most users will run these when the type of data is not known) which
does not affect the numbers in any significant way for any of the runs.

python mprsync:
```
> time python3 mprsync/sync.py -j 10 --info=progress2 --zc=zstd --zl=1 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" sumedh@$BORG_BACKUP_SERVER:vids/ vids/
Using rsync executable from /usr/bin/rsync
Running up to 10 parallel rsync jobs with paths split into 8388608 byte chunks (as per the sizes on source) ...
...
Executed in  617.28 secs    fish           external
```

mprsync.sh:
```
> time mprsync.sh -j 10 --info=progress2 --zc=zstd --zl=1 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" <remote source> <local destination>

Splitting paths having 13145.65 MB of data into 10 jobs
Running 10 parallel rsync jobs...
...
Executed in  614.17 secs    fish           external
```

rsync:
```
> time rsync --info=progress2 --zc=zstd --zl=8 --delete -aHSOJ -e "ssh -o Compression=no -c aes256-gcm@openssh.com" <remote source> <local destination>
...
Executed in   18.51 mins    fish           external
```

Here the python and bash scripts are similar and about 1.8X faster than plain rsync.
This is to be expected since the python script has an advantage due to pipelining of
path list fetch and parallel rsync data fetch phases, and the first phase has very little
cost for this case.
