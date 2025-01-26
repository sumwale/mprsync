mprsync or "multiprocess rsync" runs multiple rsync processes concurrently to sync data
much faster over slower and/or high latency networks (e.g. backups across continents)


## Features

- fetches affected files in first pass with their sizes to distribute evenly among the
  parallel rsync processes
- curates the rsync options obtained before passing to the first step to skip any
  non-desirable output, so you can use all of rsync options that will only apply to
  subsequent steps
- curates the source locations for the cases of trailing slash vs without slash, with
  or without `-R/--relative` arguments to work correctly in all cases for the fetch file
  list of the first step (sets it apart from other similar utilities out there
  that falter with different combinations of source/destination specifications)
- a last rsync at the end (optional) to ensure all metadata changes, deletes and any
  other remaining changes are applied
- no dependencies other than bash, rsync and standard POSIX utilities like sed, awk;
  soon to come python version will only need python >= 3.9

Future:

A python script will be added that does the above but with the first file fetch
and the rsync jobs running concurrently where the output of files are pipelined to the
rsync jobs on-the-fly taking into account the file sizes sent to each job so far
(e.g. prioritize the job with smallest total size so far), but also have a minimum file
size that is pushed so that many small files do not get strewn across multiple jobs.

This should improve the performance further especially for cases where there are a large
number of files to be synced having not-so-large total delta size where the first step of
file fetch in the current bash script ends up taking a huge proportion of the total time.


## Installation

Download and copy the [mprsync.sh](https://github.com/sumwale/mprsync/blob/main/mprsync.sh?raw=true)
script somewhere in your `$PATH`. In current Linux distributions a good place is
`~/.local/bin` which should be in `$PATH`. Then provide execute permission to the script
(`chmod +x mprsync.sh`).


## Usage

None of the additional options added by `mprsync.sh` script conflict with rsync options,
so you can just mix match the them with any required rsync options. The `--usage` option
details the additional options:

```
Usage: mprsync.sh [-j JOBS|--jobs=JOBS] [--skip-full-rsync] [--silent] [--usage]
       <rsync options> SRC... DEST

Run multiple rsync processes to copy local/remote files

Arguments:
  SRC...             the source location(s) for rsync
  DEST               the destination location for rsync

Options:
  -j, --jobs=JOBS    number of parallel jobs to use (default: 8)
  --skip-full-rsync  skip the full rsync at the end -- use only if no directory
                     metadata changes, or file/directory deletes are required
  --silent           don't print any informational messages
  --usage            show this help message and exit
```

The additional options need not be placed at the start before rsync ones, but it might
be clearer to do so. For example to run 10 parallel rsync jobs to sync data from a
remote server to local:

```sh
mprsync.sh -j 10 -aH --info=progress <user>@<server>:/data/ /data/
```

### CPU usage with compression

When using compression, in some cases the CPU on the server or client can turn
into a bottleneck due to multiple concurrent rsync processes. For instance, running
10 jobs with gzip compression (`-z` option to rsync) on an entry-level online storage
box or a basic online VPS storage can easily run into severe CPU bottlenecks on the
storage nodes especially for downloads requiring compression on the server.

If you are not getting expected benefits in download/upload performance compared to
plain rsync, then this may be a cause. Hence it may be better to reduce the compression
level when running `mprsync.sh` compared to the usual rsync usage and/or reduce the
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
