#!/export/home/rohitbhawal/WebMRQL/.localpython/bin/python
import sys

PROJECT_DIR = '/var/www/html/mrql'

activate_this = '/home/ubuntu/MRQL/pyvirtualenv/bin/activate_this.py'
execfile(activate_this, dict(__file__=activate_this))

sys.path.insert(0, '/var/www/html/mrql')

from mrqlserver import app as application
