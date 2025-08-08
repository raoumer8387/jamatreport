import os
import sys

# Add the app directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import the Flask app object from this file
from main import application
