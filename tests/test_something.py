import os
import sys
import pytest

sys.path.insert(1, os.path.join(sys.path[0], ".."))


from tarchia.v1 import models

print(dir(models))
