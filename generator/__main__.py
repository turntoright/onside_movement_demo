import signal
import sys

from .cli import main

# `make alerts ENV=local | head -1`: when the reader closes the pipe, end quietly like other
# command-line tools instead of a BrokenPipeError traceback. Set here, not in main(), so tests
# that call main() in process keep Python's default.
if hasattr(signal, "SIGPIPE"):  # POSIX only
    signal.signal(signal.SIGPIPE, signal.SIG_DFL)

sys.exit(main())
