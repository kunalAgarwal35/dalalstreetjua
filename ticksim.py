import os
from shutil import copyfile
import time
import datetime
tickdir = 'juneoptionticks/'
active_dir = 'temp/'
iupac = "%m%d%Y-%H%M%S-%f"
market_open = datetime.time(9,15,00)
market_close = datetime.time(15,30,00)

for f in os.listdir(active_dir):
    os.remove(os.path.join(active_dir, f))
for item in os.listdir(tickdir):
    if market_open < datetime.datetime.strptime(item,iupac).time() < market_close:
        copyfile(tickdir+item,active_dir+item)
        time.sleep(0.1)