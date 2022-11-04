import os
import sys
import time
from pathlib import Path

this_path = Path(os.path.realpath(__file__))
path_src = os.path.join(this_path.parents[3], 'src')
sys.path.insert(0, path_src)

import email_service

path_settings = os.path.join(this_path.parents[1], 'settings.json')
start_time = time.time()
email_service.main(path_settings)
print("--- %s seconds ---" % (time.time() - start_time))
