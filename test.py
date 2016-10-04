import sys
import time
import itertools

for c in itertools.cycle('/-\|'):
    sys.stdout.write('\r' + c)
    sys.stdout.flush()
    time.sleep(0.2)
