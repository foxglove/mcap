import argparse
import difflib
import os
import subprocess
import sys
from typing import List

IGNORE_DIRS = ["build"]
EXTENSIONS = [".cpp", ".hpp"]


def main(dirs: List[str], fix: bool):
    changed_paths: List[str] = []
    for root in dirs:
        for dirpath, dirnames, filenames in os.walk(root):
            # Filter out directories to skip
            dirnames[:] = filter(lambda d: d not in IGNORE_DIRS, dirnames)

            for name in filenames:
                path = os.path.join(dirpath, name)
                if any(name.endswith(ext) for ext in EXTENSIONS):
                    if fix:
                        subprocess.check_call(["clang-format", "-i", path])
                        continue

                    stdout = (
                        subprocess.check_output(["clang-format", path])
                        .decode("utf-8")
                        .splitlines()
                    )

                    with open(path, "r") as f:
                        orig = [line.rstrip("\n") for line in f]
                    diff = difflib.unified_diff(
                        orig,
                        stdout,
                        fromfile=path,
                        tofile=f"clang-format {path}",  # cspell:disable-line
                        lineterm="",
                    )
                    had_diff = False
                    for line in diff:
                        had_diff = True
                        print(line)
                    if had_diff:
                        changed_paths.append(path)
                        print("\n")

    if changed_paths:
        print(f"{len(changed_paths)} files need to be formatted:")
        for path in changed_paths:
            print(f"  {path}")
        return 1
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Run clang-format and display changed files."
    )
    parser.add_argument(
        "dirs", help="List of directories to search", nargs="+")
    parser.add_argument("--fix", action="store_true")
    args = parser.parse_args()
    sys.exit(main(**vars(args)))
