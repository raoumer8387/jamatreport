import os
import sys

# Add the application directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

# Import the Flask app
from app1 import app as application

# For debugging (remove in production)
if __name__ == '__main__':
    application.run() 