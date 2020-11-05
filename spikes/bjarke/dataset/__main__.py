from .cli import main
import traceback
import sys

try:
    main(args=sys.argv[1:])
except KeyboardInterrupt:
    print("Ctrl-C, exiting ...")
    exit(0)
except SystemExit:
    raise
except:
    print("-- error: --", file=sys.stderr)
    traceback.print_exc()
    exit(1)
