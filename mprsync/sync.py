"""
Runs multiple rsync processes concurrently to sync data much faster over slower and/or high
latency networks.

This python version fetches the paths to be updated and distributes them to new rsync processes
concurrently thus avoiding the wait for the full path list to be fetched first.
"""

import argparse
import array
import heapq
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import Future, ThreadPoolExecutor
from typing import IO, Callable, Generic, Iterable, Optional, TypeVar

FG_RED = "\033[31m"
FG_GREEN = "\033[32m"
FG_ORANGE = "\033[33m"
FG_RESET = "\033[00m"

_RSYNC_SEP = b"////"  # use a separator that cannot appear in paths
_QUEUE_SENTINEL = [b""]  # marks the end of objects being pushed in a thread queue
_DELETE_PREFIX = b"deleting "  # indicator used by rsync for files/directories to be deleted
_DELETE_SIZE = 1024  # give a fixed size of 1024 for all deletes to ensure they have some expense
# don't retry rsync threads if the only errors are those that match these patterns
# (these correspond to error codes 23 and 24 at the end)
_SKIP_RETRY_PATTERNS = re.compile(
    b"Permission denied|Operation not permitted|No such file or directory|file has vanished|"
    b"some files vanished|skipping file deletion|some files/attrs were not transferred")

T = TypeVar("T", int, float, str)


class ThreadSafeArray(Generic[T]):
    """
    Simple wrapper class for fixed size :class:`array.array` that provides just a few methods
    (get/set/update/len) that are synchronized with a lock to allow for thread-safe operations.

    :param Generic: type of the values in the array which must be supported by :class:`array.array`
    """

    def __init__(self, typecode: str, initializer: Iterable[T]):
        self._lock = threading.Lock()  # used to lock the array before read/update
        self._array = array.array(typecode, initializer)  # the underlying array

    def __len__(self) -> int:
        """get the length of the array using the builtin `len()` function"""
        return len(self._array)

    def __getitem__(self, index: int) -> T:
        """array index operator `[]` to get the value"""
        with self._lock:
            return self._array[index]

    def __setitem__(self, index: int, value: T) -> None:
        """array index operator `[]` to set the value"""
        with self._lock:
            self._array[index] = value

    def update(self, index: int, update_fn: Callable[[int, T], T]) -> T:
        """
        Update the value at given index of the array using a function/lambda that takes two
        arguments: the index of the array and current value at that index. Note that you should
        not invoke any of the get/set array operations for the same array inside the callable
        directly else it will lead to a deadlock.

        :param index: the index of the array to update
        :param update_fn: function/lambda that takes array index and current value at the index as
                          arguments and returns the new value to be set in the array
        :return: the previous value at the index in the array
        """
        with self._lock:
            value = self._array[index]
            self._array[index] = update_fn(index, value)
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

    if not (silent := bool(args.silent)):
        print_color(f"Using rsync executable from {rsync_cmd}", FG_GREEN)

    # use a min-heap to track the thread that has processed the minimum path size so far
    path_size_heap = [(0, tid) for tid in range(num_jobs)]
    heapq.heapify(path_size_heap)
    # queues to communicate list of paths to each job
    thread_queues = [queue.Queue[list[bytes]](maxsize=8192) for _ in range(num_jobs)]
    # array below is used to communicate additional path size handled by a thread due to retries
    retry_sizes = ThreadSafeArray("L", (0 for _ in range(num_jobs)))
    # futures for jobs submitted to threads
    jobs: list[Optional[Future[int]]] = [None for _ in range(num_jobs)]
    # obtain the changed path list from rsync --dry-run and distribute among the threads using
    # the min-heap above, but keep a minimum size of paths in one thread to avoid splitting
    # small files too much across threads
    rsync_fetch = build_rsync_fetch_cmd(rsync_cmd, rsync_args)
    rsync_process = build_rsync_process_cmd(rsync_cmd, rsync_args)

    failure_code = 0
    # executor for the rsync jobs
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        # reduce bufsize to not wait for too many paths
        with subprocess.Popen(rsync_fetch, bufsize=1024, stdout=subprocess.PIPE) as path_list:
            assert path_list.stdout is not None
            if not silent:
                print_color(f"Running up to {num_jobs} parallel rsync jobs splitting paths into "
                            f"{chunk_size} byte chunks (as per the sizes on source) ...", FG_GREEN)
            accumulated_size = 0  # size of the paths accumulated so far
            accumulated_paths: list[bytes] = []  # accumulated paths; drained after `chunk_size`
            while line := path_list.stdout.readline():
                # add deletes to the path list too
                if (split_idx := line.find(_RSYNC_SEP)) > 0 or line.startswith(_DELETE_PREFIX):
                    # keep minimum size of 512 since small files cause disproportionate load
                    accumulated_size += max(512, int(line[:split_idx])) if split_idx > 0 \
                        else _DELETE_SIZE
                    accumulated_paths.append(line)
                    if accumulated_size >= chunk_size:
                        _flush_accumulated(path_size_heap, accumulated_size, accumulated_paths,
                                           executor, thread_queues, retry_sizes, jobs,
                                           rsync_process, silent)
                        accumulated_size = 0
                        accumulated_paths = []  # cannot reuse since it has been put in queue
            # flush any paths left over at the end
            if len(accumulated_paths) > 0:
                _flush_accumulated(path_size_heap, accumulated_size, accumulated_paths,
                                   executor, thread_queues, retry_sizes, jobs,
                                   rsync_process, silent)
            # mark the end of objects in the thread queues
            for thread_queue in thread_queues:
                thread_queue.put(_QUEUE_SENTINEL)

            if (failure_code := path_list.wait(60)) != 0:
                if not silent:
                    print_error(f"Errors in obtaining changed paths using {rsync_fetch} "
                                "-- check the output above")

        # wait for all threads to finish
        for job_idx, job in enumerate(jobs):
            if job:
                try:
                    if (code := job.result()) != 0:
                        failure_code = code
                except Exception as ex:  # pylint: disable=broad-exception-caught
                    if not silent:
                        print_error(f"Job {job_idx} generated an exception: {ex}")
                    failure_code = 1

    if failure_code != 0:
        sys.exit(failure_code)


def _zero(idx: int, v: int) -> int:
    # pylint: disable=unused-argument
    """always returns zero"""
    return 0


def _flush_accumulated(path_list_heap: list[tuple[int, int]], accumulated_size: int,
                       accumulated_paths: list[bytes], executor: ThreadPoolExecutor,
                       thread_queues: list[queue.Queue[list[bytes]]],
                       retry_sizes: ThreadSafeArray[int], jobs: list[Optional[Future[int]]],
                       rsync_process: list[str], silent: bool) -> None:
    """
    Flush accumulated chunk into the queue of the thread having the smallest total size of paths
    so far. This will wait up to a second to put into the thread's queue else cycle through the
    remaining threads (rewinding back to the first and retrying if none succeed). It also takes
    care of any additional size processed by a thread in case of retries.
    """
    # holds threads that were popped from heap but timed out in queue put
    popped_threads: list[tuple[int, int]] = []
    # Pop the min value from heap, add size to min value, push the paths into the queue for the
    # selected thread and then push it back to the heap.
    # Also take care of:
    #   1) if there is an additional size to be added to thread due to retry, then add that after
    #      the first pop, then retry
    #   2) if put into queue times out after one second, then record it to be pushed at the end
    #      (or if the heap becomes empty) and continue with other threads in min-heap order
    while True:  # this loop retries until queue put succeeds within a second
        # this loop retries until the popped thread has no additional size to be adjusted
        # due to retries
        while True:
            if not path_list_heap:  # all threads got popped due to timeout, so retry afresh
                path_list_heap.extend(popped_threads)
                popped_threads.clear()
                heapq.heapify(path_list_heap)
            file_or_dir_size, thread_idx = heapq.heappop(path_list_heap)
            # check for additional size due to retries
            additional_size = retry_sizes.update(thread_idx, _zero)
            if additional_size > 0:
                heapq.heappush(path_list_heap, (file_or_dir_size + additional_size, thread_idx))
            else:
                break
        try:
            thread_queues[thread_idx].put(accumulated_paths, timeout=1.0)
            heapq.heappush(path_list_heap, (file_or_dir_size + accumulated_size, thread_idx))
            break
        except queue.Full:
            # record the popped thread and move to the next thread on the heap (outer while loop)
            popped_threads.append((file_or_dir_size, thread_idx))
    if popped_threads:
        for item in popped_threads:
            heapq.heappush(path_list_heap, item)
    if not jobs[thread_idx]:
        jobs[thread_idx] = executor.submit(_thread_rsync, thread_queues[thread_idx], retry_sizes,
                                           thread_idx, rsync_process, silent)


def _thread_rsync(thread_queue: queue.Queue[list[bytes]], retry_sizes: ThreadSafeArray[int],
                  thread_idx: int, rsync_process: list[str], silent: bool) -> int:
    """
    Function executed by each thread having rsync invocation with given arguments. This will also
    record the path list pushed by the main thread into the thread queue and use that to replay
    the same paths for the new rsync invocation in case the previous one failed with an unexpected
    error. The number of retries for rsync is fixed to 2 (i.e. total number of 3 tries).

    :param thread_queue: the :class:`queue.Queue` object for the thread having the paths to be
                         processed by this thread's rsync
    :param retry_sizes: the global array having the additional path size processed by retries
                        (a thread should only update the value at its own `thread_idx`)
    :param thread_idx: the index of this thread in the `retry_sizes` array
    :param rsync_process: complete rsync command-line (as a list of string) to be invoked
                          including full path of the rsync executable at the first index
    :param silent: if True then no message will be printed on standard output during retries
    :return: the final exit code of rsync process (after retries, if they were required)
    """
    # each thread task reads from its queue and writes to its rsync process (--files-from=-)
    # as well as to a temporary file so that the task can be restarted if the rsync process fails
    with tempfile.TemporaryFile("w+b", buffering=32768) as tmp_file:
        path_names = []  # initialize with anything which is not _QUEUE_SENTINEL
        exit_code = 0
        # accumulate the size seen, so that can be communicated to the main thread as the
        # additional total size read so far in case of retries
        tmp_file_size = 0
        retry_errors = bytearray()  # keep the error strings from rsync that can cause retries
        # retry up to 3 times if there is an error (e.g. due to fast ssh spawn, or connection fail)
        for retry_index in range(3):
            if retry_index > 0:
                # wait for sometime before retrying
                time.sleep(1.0 * retry_index)
                if not silent:
                    if len(retry_errors) > 512:
                        retry_errors = retry_errors[:512]
                        retry_errors.extend(b" ...")
                    print_color(f"Retry {retry_index} for job {thread_idx} due to errors:\n" +
                                retry_errors.decode("utf-8"), FG_ORANGE)
                retry_errors.clear()
                if path_names is _QUEUE_SENTINEL:  # check if queue ended in previous run
                    thread_queue.put(_QUEUE_SENTINEL)
            # reduce bufsize to not wait for too many paths
            with subprocess.Popen(rsync_process, bufsize=1024, stdin=subprocess.PIPE,
                                  stderr=subprocess.PIPE) as rsync:
                assert rsync.stdin is not None
                assert rsync.stderr is not None
                # don't block indefinitely on stderr.readline()
                os.set_blocking(rsync.stderr.fileno(), False)
                if tmp_file_size:
                    while line := tmp_file.readline():
                        rsync.stdin.write(line)
                    rsync.stdin.flush()
                    _process_stderr(rsync.stderr, retry_errors)
                    retry_sizes.update(thread_idx, lambda _, size,
                                       delta=tmp_file_size: size + delta)
                # one read from queue will have list of paths having up to `chunk_size` of data
                while (path_names := thread_queue.get()) is not _QUEUE_SENTINEL:
                    for path_name in path_names:
                        # accumulate the size read so far to add to this thread's
                        # additional "burden" in case of retries
                        split_idx = path_name.find(_RSYNC_SEP)
                        # two cases: files/dirs to be updated/created and files/dirs to be deleted
                        if split_idx > 0:
                            file_or_dir_size = max(512, int(path_name[:split_idx]))
                            path_name = path_name[split_idx + len(_RSYNC_SEP):]
                        else:
                            file_or_dir_size = _DELETE_SIZE
                            path_name = path_name[len(_DELETE_PREFIX):]
                        rsync.stdin.write(path_name)
                        tmp_file.write(path_name)
                        tmp_file_size += file_or_dir_size
                    rsync.stdin.flush()
                    _process_stderr(rsync.stderr, retry_errors)
                rsync.stdin.close()
                os.set_blocking(rsync.stderr.fileno(), True)  # start blocking on stderr at the end
                _process_stderr(rsync.stderr, retry_errors)
                if (exit_code := rsync.wait()) == 0:
                    return 0
                if (exit_code in (23, 24) and not retry_errors) or exit_code == 25:
                    return exit_code
                # read from tmp_file in the retry for tmp_file_size > 0
                tmp_file.flush()
                tmp_file.seek(0)
        return exit_code


def _process_stderr(stderr: IO[bytes], retry_errors: bytearray) -> None:
    """
    Read + flush `stderr` and store strings that should cause retries. Note that this will block
    on reads and thus potentially wait till the end of rsync process execution, so `stderr` should
    be set to non-blocking mode if this is invoked in the middle of processing.
    """
    if err_line := stderr.readline():
        while err_line:
            sys.stderr.buffer.write(err_line)
            if not _SKIP_RETRY_PATTERNS.search(err_line):
                retry_errors.extend(err_line)
            err_line = stderr.readline()
        sys.stderr.buffer.flush()  # flush only if at least one line was output


def build_rsync_fetch_cmd(rsync_cmd: str, args: list[str]) -> list[str]:
    """
    Build the complete rsync command-line to do the fetch of the path list from the remote/local
    host that will be determined by the user-provided rsync options. This will filter out or
    suppress unwanted verbosity related arguments, and do a dry run with output format to spit out
    lines having the format `<path size>_RSYNC_SEP<path>` for each path, and `deleting <path>`
    lines for paths that have to be deleted. Reader code should ignore any other lines output
    by rsync, if present.

    :param rsync_cmd: full path to the rsync executable
    :param args: user provided rsync arguments
    :return: list of strings having the complete rsync command-line
    """
    rsync_exec = [rsync_cmd]
    # remove options like --info, --debug etc from path list fetch call and negate verbosity
    for arg in args:
        if arg.startswith("--info=") or arg.startswith("--debug=") or arg == "--progress":
            continue
        rsync_exec.append(arg)
    rsync_exec.extend(("--no-v", "--dry-run", f"--out-format=%l{_RSYNC_SEP.decode('utf-8')}%n"))
    return rsync_exec


def build_rsync_process_cmd(rsync_cmd: str, args: list[str]) -> list[str]:
    """
    Build the complete rsync command-line that should be executed by each of the parallel rsync
    jobs assuming the paths as returned after running the rsync command returned by
    `build_rsync_fetch_cmd` and assumes that the path list will be provided to the rsync process
    on its standard input. This will take care of curating the user provided rsync arguments
    to deal correctly with relative paths (`-R/--relative` rsync option), or source paths ending
    in slash or without slash. It also adds option to ignore errors for missing paths on source
    in case of deletes (`--delete*` rsync options).

    :param rsync_cmd: full path to the rsync executable
    :param args: user provided rsync arguments
    :return: list of strings having the complete rsync command-line
    """
    # Adjust source paths as per expected path list received above which is that if there
    # is no trailing slash, then path list contains the last element hence remove it.
    # When -R/--relative is being used then path names provided are full names hence
    # trim the paths in the source till root "/" or "."
    rsync_process = [rsync_cmd]
    is_relative_re = re.compile(r"^-[^-]*R|^--relative$")
    trim_for_relpath = re.compile(r"(^|:)([^:]).*$")
    trim_url_for_relpath = re.compile(r"^(rsync://[^/]*/)(.).*")
    trim_for_noslash = re.compile(r"(^|[/:])[^/:]*$")

    def sub_relative(match: re.Match[str]) -> str:
        return match.group(1) + ("/" if match.group(2) == "/" else ".")

    # track and process previous arg since the last positional arg of destination has to be skipped
    prev_arg = ""
    is_relative = False
    has_delete = False
    for arg in args:
        if len(arg) == 0 or arg[0] == "-":
            rsync_process.append(arg)
            if arg.startswith("--delete"):
                if not has_delete and arg != "--delete-missing-args":
                    has_delete = True
            elif not is_relative:
                is_relative = is_relative_re.match(arg) is not None
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
        msg = f"{fg}{msg}{FG_RESET}"
    # force flush the output if it doesn't end in a newline
    print(msg, end=end, flush=end != "\n")


def print_error(msg: str, end: str = "\n") -> None:
    """
    Display given string to standard output as an error in foreground color red.

    :param msg: the string to be displayed
    :param end: the terminating string which is newline by default (or can be empty for example)
    """
    print_color(msg, FG_RED, end)


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """
    Parse command-line arguments for the program and return a tuple of parsed
    :class:`argparse.Namespace` and the list of unparsed arguments that should be passed over
    to rsync.

    :param argv: the list of arguments to be parsed
    :return: the result of parsing using the `argparse` library as a tuple of parsed
             :class:`argparse.Namespace` and unparsed list of strings
    """
    parser = argparse.ArgumentParser(
        description="Run multiple rsync processes to copy local/remote files and directories")
    parser.add_argument("-j", "--jobs", type=int, default=8, help="number of parallel jobs to use")
    parser.add_argument("--chunk-size", type=int, default=8 * 1024 * 1024,
                        help="minimum chunk size (in bytes) for splitting paths among the jobs")
    parser.add_argument("--silent", action="store_true",
                        help="don't print any informational messages from this program (does not "
                             "affect rsync output which is governed by its own flags)")
    return parser.parse_known_args(argv)


if __name__ == "__main__":
    main()
