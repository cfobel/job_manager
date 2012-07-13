import sys
from path import path

def setup_env():
    root = path(__file__).parent.parent
    sys.path.insert(0, root)
