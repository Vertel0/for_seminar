import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SOLUTION = os.path.dirname(HERE)
ROOT = os.path.dirname(SOLUTION)
sys.path.insert(0, os.path.join(ROOT, "lab"))   # intake_service, station_gen, measure
sys.path.insert(0, SOLUTION)                     # client_naive/parallel/reliable
