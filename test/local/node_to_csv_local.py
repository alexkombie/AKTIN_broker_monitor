import os
import src.node_to_csv
import time

path_parent = os.path.dirname(os.getcwd())
path_local = os.path.join(path_parent, 'local')
path_settings = os.path.join(path_local, 'settings.json')

start_time = time.time()
src.node_to_csv.main(path_settings)
print("--- %s seconds ---" % (time.time() - start_time))
