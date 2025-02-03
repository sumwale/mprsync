"""
Runs multiple rsync processes concurrently to sync data much faster over slower and/or high
latency networks.

This python version fetches the files to be updated and distributes them to new rsync processes
concurrently thus avoiding the wait for the full file list to be fetched first.
"""

import argparse
import heapq
import queue
import re
import shutil
import subprocess
import sys
from concurrent.futures import Future, ThreadPoolExecutor
from typing import Any, Optional

fg_red = "\033[31m"
fg_green = "\033[32m"
fg_orange = "\033[33m"
fg_reset = "\033[00m"

_QUEUE_SENTINEL = object()  # marks the end of objects being pushed in a thread queue


def main() -> None:
    """main function for `mprsync`"""
    args, rsync_args = parse_args(sys.argv[1:])
    if num_jobs := int(args.jobs) <= 0:
        print_error(f"Expected positive value for -j/--jobs option but got {num_jobs}")
        sys.exit(1)
    if not (rsync_cmd := shutil.which("rsync")):
        print_error("No rsync found in PATH")
        sys.exit(1)

    if not args.silent:
        print_color(f"Using rsync executable from {rsync_cmd}", fg_green)
        print_color(f"Running up to {num_jobs} parallel rsync jobs ...", fg_green)

    # use a min-heap to track the thread that has processed the minimum file size so far
    file_list_heap = [(0, tid) for tid in range(num_jobs)]
    heapq.heapify(file_list_heap)
    # queues to communicate list of files to each job
    queue_size = 256 * 1024
    thread_queues = [queue.Queue[Any](maxsize=queue_size) for _ in range(num_jobs)]
    # futures for jobs submitted to threads
    jobs: list[Optional[Future[None]]] = [None for _ in range(num_jobs)]
    # obtain the changed file list from rsync --dry-run and distribute among the threads using
    # the min-heap above, but keep a minimum size of files in one thread to avoid splitting
    # small files too much across threads
    rsync_out_sep = "////"  # use a separator that cannot appear in file paths
    # max accumulated file size after which file names are written to selected thread's queue
    flush_size = 2 * 1024 * 1024
    rsync_fetch, is_relative = build_rsync_fetch_cmd(rsync_cmd, rsync_args, rsync_out_sep)
    rsync_process = build_rsync_process_cmd(rsync_cmd, rsync_args, is_relative)

    failure_code = 0
    # executor for the rsync jobs
    with ThreadPoolExecutor(max_workers=num_jobs) as executor:
        # reduce bufsize to not wait for too many file names
        with subprocess.Popen(rsync_fetch, bufsize=512, stdout=subprocess.PIPE) as file_list:
            assert file_list.stdout is not None
            accumulated_file_size = 0  # size of the files accumulated so far
            accumulated_num_files = 0  # number of files accumulated so far
            accumulated_files: list[str] = []  # accumulated files drained on reaching `flush_size`
            while line := file_list.stdout.readline():
                line_str = line.decode("utf-8")
                if (split_index := line_str.find(rsync_out_sep)) > 0:
                    # pretend size 0 as 1, so that zero sized files still cause it to change
                    accumulated_file_size += max(1, int(line_str[:split_index]))
                    accumulated_files.append(line_str[split_index + len(rsync_out_sep):])
                    if accumulated_file_size >= flush_size or accumulated_num_files >= queue_size:
                        _flush_accumulated(file_list_heap, accumulated_file_size,
                                           accumulated_files, executor, thread_queues, jobs,
                                           rsync_process)
                        accumulated_file_size = 0
                        accumulated_num_files = 0
                        accumulated_files.clear()
            # flush any files left over at the end
            if accumulated_num_files > 0:
                _flush_accumulated(file_list_heap, accumulated_file_size,
                                   accumulated_files, executor, thread_queues, jobs, rsync_process)
            # mark the end of objects in the thread queues
            for thread_queue in thread_queues:
                thread_queue.put(_QUEUE_SENTINEL)

            if (failure_code := file_list.wait(60)) != 0:
                print_error("FAILED to obtain changed file list -- check the output above")

        # wait for all threads to finish
        for job_idx, job in enumerate(jobs):
            if job:
                try:
                    job.result()
                except Exception as ex:
                    print_error(f"Job {job_idx} generated an exception: {ex}")
                    failure_code = 1

    if failure_code != 0:
        # TODO: remove temporary files before exit
        sys.exit(failure_code)


def _flush_accumulated(file_list_heap: list[tuple[int, int]], acc_file_size: int,
                       acc_files: list[str], executor: ThreadPoolExecutor,
                       thread_queues: list[queue.Queue[Any]], jobs: list[Optional[Future[None]]],
                       rsync_process: list[str]) -> None:
    # pop the min value from heap, add size to min value, push it back to the heap and
    # push the file names into the queue for the selected thread
    file_size, thread_idx = heapq.heappop(file_list_heap)
    heapq.heappush(file_list_heap, (file_size + acc_file_size, thread_idx))
    for file_name in acc_files:
        thread_queues[thread_idx].put(file_name)
    if not jobs[thread_idx]:
        jobs[thread_idx] = executor.submit(_thread_rsync, thread_queues[thread_idx], rsync_process)


def _thread_rsync(thread_queue: queue.Queue[Any], rsync_process: list[str]) -> None:
    # each thread task reads from its queue and writes to its rsync process (--files-from=-)
    # as well as to a temporary file so that the task can be restarted if the rsync process fails
    with subprocess.Popen(rsync_process, stdin=subprocess.PIPE) as read_files:
        assert read_files.stdin is not None
        while (file_name := thread_queue.get()) is not _QUEUE_SENTINEL:
            # read all available file names and flush the writes once at the end
            while True:
                read_files.stdin.write(str(file_name).encode("utf-8"))
                try:
                    if (file_name := thread_queue.get_nowait()) is _QUEUE_SENTINEL:
                        break
                except queue.Empty:
                    break
            read_files.stdin.flush()
            if file_name is _QUEUE_SENTINEL:
                break


def build_rsync_fetch_cmd(rsync_cmd: str, args: list[str], sep: str) -> tuple[list[str], bool]:
    rsync_exec = [rsync_cmd]
    is_relative = False
    is_relative_re = re.compile(r"^-[^-]*R|^--relative$")
    # remove options like --info, --debug etc from file list fetch call and negate verbosity
    for arg in args:
        if arg.startswith("--info=") or arg.startswith("--debug=") or arg == "--progress":
            continue
        rsync_exec.append(arg)
        is_relative = is_relative or (is_relative_re.match(arg) is not None)
    rsync_exec.extend(("--no-v", "--dry-run", f"--out-format=%l{sep}%n"))
    return rsync_exec, is_relative


def build_rsync_process_cmd(rsync_cmd: str, args: list[str], is_relative: bool) -> list[str]:
    # Adjust source paths as per expected file list received above which is that if there
    # is no trailing slash, then file list contains the last element hence remove it.
    # When -R/--relative is being used then path names provided are full names hence
    # trim the paths in the source till root "/" or "."
    rsync_process = [rsync_cmd]
    trim_relative_path = re.compile(r"(^|:)([^:]).*")
    trim_rsync_url = re.compile(r"^(rsync://[^/]*/)(.).*")

    def sub_relative(match: re.Match[str]) -> str:
        return match.group(1) + ("/" if match.group(2) == "/" else ".")

    # track and process previous arg since the last positional arg of destination has to be skipped
    prev_arg = ""
    for arg in args:
        if len(arg) == 0 or arg[0] == "-":
            rsync_process.append(arg)
            continue
        if prev_arg:
            if is_relative:
                # for this case the file names already have the full paths,
                # so the source should be trimmed all the way till the root (i.e. "/" or ".")
                if prev_arg.startswith("rsync://"):
                    prev_arg = trim_rsync_url.sub(sub_relative, prev_arg)
                else:
                    prev_arg = trim_relative_path.sub(sub_relative, prev_arg)
            elif prev_arg[len(prev_arg) - 1] != "/":
                # if the source does not end in a slash, then file list obtained above will
                # already contain the last directory of the path, hence remove it
                # TODO: check the case of <user>@server:<dir> where <dir> has no slashes at all
                if (last_slash_idx := prev_arg.rfind("/")) != -1:
                    prev_arg = prev_arg[:last_slash_idx]
            rsync_process.append(prev_arg)
        prev_arg = arg

    if prev_arg:
        rsync_process.append(prev_arg)
    # inhibit recursive flag since each file/directory needs to be processed separately
    rsync_process.append("--no-r")
    rsync_process.append("--files-from=-")  # read the files from stdin written to by each thread
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
        description="Run multiple rsync processes to copy local/remote files", add_help=False)
    parser.add_argument("-j", "--jobs", type=int, default=8, help="number of parallel jobs to use")
    parser.add_argument("--skip-full-rsync", action="store_true",
                        help="skip the full rsync at the end -- use only if no directory "
                        "metadata changes, or file/directory deletes are required")
    parser.add_argument("--silent", action="store_true",
                        help="don't print any informational messages")
    parser.add_argument("--usage", action="store_true", help="show this help message and exit")
    result = parser.parse_known_args(argv)
    # check for --usage
    if result[0].usage:
        parser.print_help()
        sys.exit(0)
    return result


if __name__ == "__main__":
    main()
