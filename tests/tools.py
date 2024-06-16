"""
Test Harness

This module provides a set of utility functions and decorators to assist with
conditional test execution, platform-specific checks, and test result reporting
when running pytest tests locally. 

It includes functionality to:

- Check the platform and Python implementation (e.g., ARM architecture, Windows, macOS, PyPy).
- Conditionally skip tests based on platform, Python version, or environment variables.
- Download files and determine character display widths.
- Truncate and format printable strings.
- Discover and run test functions from the calling module, capturing output and providing detailed
  pass/fail status reports.

The primary entry point is the `run_tests` function, which discovers and executes all functions in
the calling module whose names start with 'test_', capturing their output and reporting the results
in a formatted manner.

Functions:
    is_arm(): Check if the current platform is ARM architecture.
    is_windows(): Check if the current platform is Windows.
    is_mac(): Check if the current platform is macOS.
    is_pypy(): Check if the current Python implementation is PyPy.
    manual(): Check if manual testing is enabled via the MANUAL_TEST environment variable.
    is_version(version): Check if the current Python version matches the specified version.
    skip(func): Decorator to skip the execution of a test function and issue a warning.
    skip_if(is_true=True): Decorator to conditionally skip the execution of a test function based on a condition.
    download_file(url, path): Download a file from a given URL and save it to a specified path.
    character_width(symbol): Determine the display width of a character based on its Unicode East Asian Width property.
    trunc_printable(value, width, full_line=True): Truncate a string to fit within a specified width, accounting for character widths.
    run_tests(): Discover and run test functions defined in the calling module.

Usage:
    To use this module, define your test functions in the calling module with names starting with 'test_'.
    Then call `run_tests()` to execute them and display the results.

Example:
    # In your test module
    def test_example():
        assert True

    if __name__ == "__main__":
        run_tests()
"""

import platform
from functools import wraps


def is_arm():  # pragma: no cover
    """
    Check if the current platform is ARM architecture.

    Returns:
        bool: True if the platform is ARM, False otherwise.
    """
    return platform.machine() in ("armv7l", "aarch64")


def is_windows():  # pragma: no cover
    """
    Check if the current platform is Windows.

    Returns:
        bool: True if the platform is Windows, False otherwise.
    """
    return platform.system().lower() == "windows"


def is_mac():  # pragma: no cover
    """
    Check if the current platform is macOS.

    Returns:
        bool: True if the platform is macOS, False otherwise.
    """
    return platform.system().lower() == "darwin"


def is_pypy():  # pragma: no cover
    """
    Check if the current Python implementation is PyPy.

    Returns:
        bool: True if the Python implementation is PyPy, False otherwise.
    """
    return platform.python_implementation() == "PyPy"


def manual():  # pragma: no cover
    """
    Check if manual testing is enabled via the MANUAL_TEST environment variable.

    Returns:
        bool: True if MANUAL_TEST environment variable is set, False otherwise.
    """
    import os

    return os.environ.get("MANUAL_TEST") is not None


def is_version(version: str) -> bool:
    """
    Check if the current Python version matches the specified version.

    Parameters:
        version (str): The version string to check against.

    Returns:
        bool: True if the current Python version matches, False otherwise.

    Raises:
        Exception: If the version string is empty.
    """
    import sys

    if len(version) == 0:
        raise Exception("is_version needs a version")
    if version[-1] != ".":
        version += "."
    print(sys.version)
    return (sys.version.split(" ")[0] + ".").startswith(version)


def skip(func):  # pragma: no cover
    """
    Decorator to skip the execution of a test function and issue a warning.

    Parameters:
        func (Callable): The test function to skip.

    Returns:
        Callable: The wrapped function that issues a warning.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        import warnings

        warnings.warn(f"Skipping {func.__name__}")

    return wrapper


def skip_if(is_true: bool = True):
    """
    Decorator to conditionally skip the execution of a test function based on a condition.

    Parameters:
        is_true (bool): Condition to skip the function. Defaults to True.

    Returns:
        Callable: The decorator that conditionally skips the test function.

    Example:
        I want to skip this test on ARM machines:

            @skip_if(is_arm()):
            def test...

        I want to skip this test on Windows machines running Python 3.8

            @skip_if(is_windows() and is_version("3.8"))
            def test...
    """
    def decorate(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            if is_true and not manual():
                import warnings

                warnings.warn(f"Skipping {func.__name__} because of conditional execution.")
            else:
                return func(*args, **kwargs)

        return wrapper

    return decorate


def download_file(url: str, path: str):
    """
    Download a file from a given URL and save it to a specified path.

    Parameters:
        url (str): The URL to download the file from.
        path (str): The path to save the downloaded file.

    Returns:
        None
    """
    import requests

    response = requests.get(url)
    with open(path, "wb") as f:
        f.write(response.content)
    print(f"Saved downloaded contents to {path}")


def character_width(symbol: str) -> int:
    """
    Determine the display width of a character based on its Unicode East Asian Width property.

    Parameters:
        symbol (str): The character to measure.

    Returns:
        int: The width of the character (1 or 2).
    """
    import unicodedata

    return 2 if unicodedata.east_asian_width(symbol) in ("F", "N", "W") else 1


def trunc_printable(value: str, width: int, full_line: bool = True) -> str:
    """
    Truncate a string to fit within a specified width, accounting for character widths.

    Parameters:
        value (str): The string to truncate.
        width (int): The maximum display width.
        full_line (bool): Whether to pad the string to the full width. Defaults to True.

    Returns:
        str: The truncated string.
    """
    if not isinstance(value, str):
        value = str(value)

    offset = 0
    emit = ""
    ignoring = False

    for char in value:
        if char == "\n":
            emit += "↵"
            offset += 1
            continue
        if char == "\r":
            continue
        emit += char
        if char == "\033":
            ignoring = True
        if not ignoring:
            offset += character_width(char)
        if ignoring and char == "m":
            ignoring = False
        if not ignoring and offset >= width:
            return emit + "\033[0m"
    line = emit + "\033[0m"
    if full_line:
        return line + " " * (width - offset)
    return line


def run_tests():
    """
    Discover and run test functions defined in the calling module. Test functions should be named starting with 'test_'.

    This function captures the output of each test, reports pass/fail status, and provides detailed error information if a test fails.

    Returns:
        None
    """
    import contextlib
    import inspect
    import os
    import shutil
    import time
    import traceback
    from io import StringIO

    OS_SEP = os.sep

    manual_test = os.environ.get("MANUAL_TEST")
    os.environ["MANUAL_TEST"] = "1"

    display_width = shutil.get_terminal_size((80, 20))[0]

    # Get the calling module
    caller_module = inspect.getmodule(inspect.currentframe().f_back)
    test_methods = []
    for name, obj in inspect.getmembers(caller_module):
        if inspect.isfunction(obj) and name.startswith("test_"):
            test_methods.append(obj)

    print(f"\n\033[38;2;139;233;253m\033[3mRUNNING SET OF {len(test_methods)} TESTS\033[0m\n")
    start_suite = time.monotonic_ns()

    passed = 0
    failed = 0

    for index, method in enumerate(test_methods):
        start_time = time.monotonic_ns()
        test_name = f"\033[38;2;255;184;108m{(index + 1):04}\033[0m \033[38;2;189;147;249m{str(method.__name__)}\033[0m"
        print(test_name.ljust(display_width - 20), end="", flush=True)
        error = None
        output = ""
        try:
            stdout = StringIO()  # Create a StringIO object
            with contextlib.redirect_stdout(stdout):
                method()
            output = stdout.getvalue()
        except Exception as err:
            error = err
        finally:
            if error is None:
                passed += 1
                status = "\033[38;2;26;185;67m pass"
            else:
                failed += 1
                status = "\033[38;2;255;121;198m fail"
        time_taken = int((time.monotonic_ns() - start_time) / 1e6)
        print(f"\033[0;32m{str(time_taken).rjust(8)}ms {status}\033[0m")
        if error:
            traceback_details = traceback.extract_tb(error.__traceback__)
            file_name, line_number, function_name, code_line = traceback_details[-1]
            file_name = file_name.split(OS_SEP)[-1]
            print(
                f"  \033[38;2;255;121;198m{error.__class__.__name__}\033[0m"
                + f" {error}\n"
                + f"  \033[38;2;241;250;140m{file_name}\033[0m"
                + "\033[38;2;98;114;164m:\033[0m"
                + f"\033[38;2;26;185;67m{line_number}\033[0m"
                + f" \033[38;2;98;114;164m{code_line}\033[0m"
            )
        if output:
            print(
                "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
                + output.strip()
                + "\n"
                + "\033[38;2;98;114;164m"
                + "=" * display_width
                + "\033[0m"
            )

    print(
        f"\n\033[38;2;139;233;253m\033[3mCOMPLETE\033[0m ({((time.monotonic_ns() - start_suite) / 1e9):.2f} seconds)\n"
        f"  \033[38;2;26;185;67m{passed} passed ({(passed * 100) // (passed + failed)}%)\033[0m\n"
        f"  \033[38;2;255;121;198m{failed} failed\033[0m"
    )