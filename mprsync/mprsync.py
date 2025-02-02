"""
Runs multiple rsync processes concurrently to sync data much faster over slower and/or high
latency networks.

This python version fetches the files to be updated and distributes them to new rsync processes
concurrently thus avoiding the wait for the full file list to be fetched first.
"""

import argparse
import sys


def main() -> None:
    """main function for `mprsync`"""
    args, remaining = parse_args(sys.argv[1:])
    jobs = int(args.jobs)
    if not args.silent:
        print(f"Running rsync with {jobs} rsync processes and arguments: {remaining}")


def parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
    """
    Parse command-line arguments for the program and return a tuple of parsed
    `argparse.Namespace` and the list of unparsed arguments that should be passed over to rsync.
    The `--usage` argument is processed directly to show the help message and exit.

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
    if result[0].usage:
        parser.print_help()
        sys.exit(0)
    return result


if __name__ == "__main__":
    main()
