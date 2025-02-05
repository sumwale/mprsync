"""
Runs multiple rsync processes concurrently to sync data much faster over slower and/or high
latency networks.

This python version fetches the paths to be updated and distributes them to new rsync processes
concurrently thus avoiding the wait for the full path list to be fetched first.
"""

import argparse
import array
import heapq
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from io import BytesIO
from typing import Callable, Generic, Iterable, Optional, TypeVar

fg_red = "\033[31m"
fg_green = "\033[32m"
fg_reset = "\033[00m"

_RSYNC_SEP = b"////"  # use a separator that cannot appear in paths
_QUEUE_SENTINEL = b""  # marks the end of objects being pushed in a thread queue
_DELETE_PREFIX = b"deleting "  # indicator used by rsync for files/directories to be deleted
_DELETE_SIZE = 1024  # give a fixed size of 1024 for all deletes to ensure they have some expense


T = TypeVar("T", int, float, str)


class ThreadSafeArray(Generic[T]):
    def __init__(self, typecode: str, initializer: Iterable[T]):
        self._lock = threading.Lock()  # used to lock the array before read/update
        self._array = array.array(typecode, initializer)

    def __len__(self) -> int:
        return len(self._array)

    def __getitem__(self, idx: int) -> T:
        with self._lock:
            return self._array[idx]

    def __setitem__(self, idx: int, value: T) -> None:
        with self._lock:
            self._array[idx] = value

    def update(self, idx: int, update_fn: Callable[[int, T], T]) -> T:
        with self._lock:
            value = self._array[idx]
            self._array[idx] = update_fn(idx, value)
            return value


def main() -> None:
    """main function for `mprsync`"""
    args, rsync_args = parse_args(sys.argv[1:])
    if (num_jobs := int(args.jobs)) <= 0:
        print_error(f"Expected positive value for -j/--jobs option but got {num_jobs}")
        sys.exit(1)
    if (chunk_size := int(args.chunk_size)) <= 0:
        print_error(f"Expected positive value for --chunk-size option but got {chunk_size}")
        sys.exit(1)
    if not (rsync_cmd := shutil.which("rsync")):
        print_error("No rsync found in PATH")
        sys.exit(1)

    if not args.silent:
        print_color(f"Using rsync executable from {rsync_cmd}", fg_green)

    # use a min-heap to track the thread that has processed the minimum path size so far
    path_size_heap = [(0, tid) for tid in range(num_jobs)]
    heapq.heapify(path_size_heap)
    # queues to communicate list of paths to each job
    queue_size = 256 * 1024
    thread_queues = [queue.Queue[bytes](maxsize=queue_size) for _ in range(num_jobs)]
    # array below is used to communicate additional path size handled by a thread due to retries
    retry_sizes = ThreadSafeArray("L", (0 for _ in range(num_jobs)))
    # futures for jobs submitted to threads
    jobs: list[Optional[Future[int]]] = [None for _ in range(num_jobs)]
    # obtain the changed path list from rsync --dry-run and distribute among the threads using
    # the min-heap above, but keep a minimum size of paths in one thread to avoid splitting
    # small files too much across threads
    rsync_fetch, is_relative = build_rsync_fetch_cmd(rsync_cmd, rsync_args)
    rsync_process = build_rsync_process_cmd(rsync_cmd, rsync_args, is_relative)

    failure_code = 0
    # executor for the rsync jobs
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        # reduce bufsize to not wait for too many paths
        with subprocess.Popen(rsync_fetch, bufsize=1024, stdout=subprocess.PIPE) as path_list:
            assert path_list.stdout is not None
            if not args.silent:
                print_color(f"Running up to {num_jobs} parallel rsync jobs splitting files into "
                            f"{chunk_size} chunks ...", fg_green)
            accumulated_size = 0  # size of the paths accumulated so far
            accumulated_paths: list[bytes] = []  # accumulated paths; drained after `chunk_size`
            while line := path_list.stdout.readline():
                # add deletes to the path list too
                if (split_idx := line.find(_RSYNC_SEP)) > 0 or line.startswith(_DELETE_PREFIX):
                    # pretend size 0 as 1, so that zero sized files still cause it to change
                    accumulated_size += max(1, int(line[:split_idx])) if split_idx > 0 \
                        else _DELETE_SIZE
                    accumulated_paths.append(line)
                    if accumulated_size >= chunk_size or len(accumulated_paths) >= queue_size:
                        _flush_accumulated(path_size_heap, accumulated_size, accumulated_paths,
                                           executor, thread_queues, retry_sizes, jobs,
                                           rsync_process)
                        accumulated_size = 0
                        accumulated_paths.clear()
            # flush any paths left over at the end
            if len(accumulated_paths) > 0:
                _flush_accumulated(path_size_heap, accumulated_size, accumulated_paths,
                                   executor, thread_queues, retry_sizes, jobs, rsync_process)
            # mark the end of objects in the thread queues
            for thread_queue in thread_queues:
                thread_queue.put(_QUEUE_SENTINEL)

            if (failure_code := path_list.wait(60)) != 0:
                if not args.silent:
                    print_error(f"Errors in obtaining changed paths using {rsync_fetch} "
                                "-- check the output above")

        # wait for all threads to finish
        for job_idx, job in enumerate(jobs):
            if job:
                try:
                    if (code := job.result()) != 0:
                        failure_code = code
                except Exception as ex:
                    if not args.silent:
                        print_error(f"Job {job_idx} generated an exception: {ex}")
                    failure_code = 1

    if failure_code != 0:
        sys.exit(failure_code)


def _set_zero(idx: int, v: int) -> int:  # type: ignore
    return 0


def _flush_accumulated(path_list_heap: list[tuple[int, int]], accumulated_size: int,
                       accumulated_paths: list[bytes], executor: ThreadPoolExecutor,
                       thread_queues: list[queue.Queue[bytes]], retry_sizes: ThreadSafeArray[int],
                       jobs: list[Optional[Future[int]]], rsync_process: list[str]) -> None:
    # pop the min value from heap, add size to min value, push it back to the heap and
    # push the paths into the queue for the selected thread
    while True:
        file_or_dir_size, thread_idx = heapq.heappop(path_list_heap)
        # check for additional size due to retries
        additional_size = retry_sizes.update(thread_idx, _set_zero)
        if additional_size > 0:
            heapq.heappush(path_list_heap, (file_or_dir_size + additional_size, thread_idx))
        else:
            break
    heapq.heappush(path_list_heap, (file_or_dir_size + accumulated_size, thread_idx))
    thread_queues[thread_idx].put(b"".join(accumulated_paths))
    if not jobs[thread_idx]:
        jobs[thread_idx] = executor.submit(_thread_rsync, thread_queues[thread_idx], retry_sizes,
                                           thread_idx, rsync_process)


def _thread_rsync(thread_queue: queue.Queue[bytes], retry_sizes: ThreadSafeArray[int],
                  thread_idx: int, rsync_process: list[str]) -> int:
    # each thread task reads from its queue and writes to its rsync process (--files-from=-)
    # as well as to a temporary file so that the task can be restarted if the rsync process fails
    with tempfile.TemporaryFile("w+b") as tmp_file:
        exit_code = 0
        # accumulate the size seen, so that can be communicated to the main thread as the
        # additional total size read so far in case of retries
        tmp_file_size = 0
        # retry up to 3 times if there is an error (e.g. due to fast ssh spawn, or connection fail)
        for retry_count in range(3):
            if retry_count > 0:
                # wait for sometime before retrying
                time.sleep(1.0 * retry_count)
            # reduce bufsize to not wait for too many paths
            with subprocess.Popen(rsync_process, bufsize=1024, stdin=subprocess.PIPE) as rsync:
                assert rsync.stdin is not None
                if tmp_file_size:
                    while line := tmp_file.readline():
                        rsync.stdin.write(line)
                    rsync.stdin.flush()
                    retry_sizes.update(thread_idx, lambda _, size,
                                       delta=tmp_file_size: size + delta)
                    tmp_file_size = 0
                # instead of reading one path at a time and flushing, read all available using
                # get_nowait() and flush once when there is nothing left in queue
                while (path_names := thread_queue.get()) is not _QUEUE_SENTINEL:
                    path_names_buffer = BytesIO(path_names)
                    while path_name := path_names_buffer.readline():
                        # accumulate the size read so far to add to this thread's
                        # additional "burden" in case of retries
                        split_idx = path_name.find(_RSYNC_SEP)
                        # two cases: files/dirs to be updated/created and files/dirs to be deleted
                        if split_idx > 0:
                            file_or_dir_size = max(1, int(path_name[:split_idx]))
                            path_name = path_name[split_idx + len(_RSYNC_SEP):]
                        else:
                            file_or_dir_size = _DELETE_SIZE
                            path_name = path_name[len(_DELETE_PREFIX):]
                        rsync.stdin.write(path_name)
                        tmp_file.write(path_name)
                        tmp_file_size += file_or_dir_size
                    rsync.stdin.flush()
                rsync.stdin.close()
                # TODO: don't retry if the only errors were permission denied, failed to set etc
                # all of which will be code 23, so need to check stderr in addition
                if (exit_code := rsync.wait()) == 0:
                    return 0
                # read from tmp_file at the start of the retry loop
                tmp_file.flush()
                tmp_file.seek(0)
        return exit_code


def build_rsync_fetch_cmd(rsync_cmd: str, args: list[str]) -> tuple[list[str], bool]:
    rsync_exec = [rsync_cmd]
    is_relative = False
    is_relative_re = re.compile(r"^-[^-]*R|^--relative$")
    # remove options like --info, --debug etc from path list fetch call and negate verbosity
    for arg in args:
        if arg.startswith("--info=") or arg.startswith("--debug=") or arg == "--progress":
            continue
        rsync_exec.append(arg)
        is_relative = is_relative or (is_relative_re.match(arg) is not None)
    rsync_exec.extend(("--no-v", "--dry-run", f"--out-format=%l{_RSYNC_SEP.decode('utf-8')}%n"))
    return rsync_exec, is_relative


def build_rsync_process_cmd(rsync_cmd: str, args: list[str], is_relative: bool) -> list[str]:
    # Adjust source paths as per expected path list received above which is that if there
    # is no trailing slash, then path list contains the last element hence remove it.
    # When -R/--relative is being used then path names provided are full names hence
    # trim the paths in the source till root "/" or "."
    rsync_process = [rsync_cmd]
    trim_for_relpath = re.compile(r"(^|:)([^:]).*")
    trim_url_for_relpath = re.compile(r"^(rsync://[^/]*/)(.).*")
    trim_for_noslash = re.compile(r"(^|[/:])[^/:]*")

    def sub_relative(match: re.Match[str]) -> str:
        return match.group(1) + ("/" if match.group(2) == "/" else ".")

    # track and process previous arg since the last positional arg of destination has to be skipped
    prev_arg = ""
    has_delete = False
    for arg in args:
        if len(arg) == 0 or arg[0] == "-":
            rsync_process.append(arg)
            if not has_delete and arg.startswith("--delete") and arg != "--delete-missing-args":
                has_delete = True
            continue
        if prev_arg:
            if is_relative:
                # for this case the paths already have the full paths,
                # so the source should be trimmed all the way till the root (i.e. "/" or ".")
                if prev_arg.startswith("rsync://"):
                    prev_arg = trim_url_for_relpath.sub(sub_relative, prev_arg)
                else:
                    prev_arg = trim_for_relpath.sub(sub_relative, prev_arg)
            elif prev_arg[len(prev_arg) - 1] != "/":
                # if the source does not end in a slash, then path list obtained above will
                # already contain the last directory of the path, hence remove it
                prev_arg = trim_for_noslash.sub(r"\1.", prev_arg)
            rsync_process.append(prev_arg)
        prev_arg = arg

    if prev_arg:
        rsync_process.append(prev_arg)
    # inhibit recursive flag since each file/directory needs to be processed separately
    rsync_process.append("--no-r")  # implied by --files-from but still adding it for clarity
    rsync_process.append("--files-from=-")  # read the paths from stdin written to by each thread
    if has_delete:
        # ignore missing paths in --files-from on source for paths to be deleted on receiver
        rsync_process.append("--ignore-missing-args")
    return rsync_process


def print_color(msg: str, fg: Optional[str] = None, end: str = "\n") -> None:
    """
    Display given string to standard output in foreground color, if provided.

    :param msg: the string to be displayed
    :param fg: the foreground color of the string
    :param end: the terminating string which is newline by default (or can be empty for example)
    """
    if fg:
        msg = f"{fg}{msg}{fg_reset}"
    # force flush the output if it doesn't end in a newline
    print(msg, end=end, flush=end != "\n")


def print_error(msg: str, end: str = "\n") -> None:
    """
    Display given string to standard output as an error in foreground color red.

    :param msg: the string to be displayed
    :param end: the terminating string which is newline by default (or can be empty for example)
    """
    print_color(msg, fg_red, end)


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """
    Parse command-line arguments for the program and return a tuple of parsed
    :class:`argparse.Namespace` and the list of unparsed arguments that should be passed over
    to rsync. The `--usage` argument is processed directly to show the help message and exit.

    :param argv: the list of arguments to be parsed
    :return: the result of parsing using the `argparse` library as a tuple of parsed
             :class:`argparse.Namespace` and unparsed list of strings
    """
    parser = argparse.ArgumentParser(
        description="Run multiple rsync processes to copy local/remote files and directories")
    parser.add_argument("-j", "--jobs", type=int, default=8, help="number of parallel jobs to use")
    parser.add_argument("--chunk-size", type=int, default=2 * 1024 * 1024,
                        help="minimum chunk size (in bytes) for splitting files among the jobs")
    parser.add_argument("--silent", action="store_true",
                        help="don't print any informational messages from this program (does not "
                             "affect rsync output which is governed by its own flags)")
    return parser.parse_known_args(argv)


if __name__ == "__main__":
    main()
