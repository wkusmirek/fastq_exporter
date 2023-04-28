import schedule
import time
import shutil
import os

files = os.listdir('data/guppy/splitted/')

def job():
    shutil.copyfile('data/guppy/splitted/'+files[0], '/tmp/fastq/'+files[0])
    files.pop(0)

schedule.every(10).seconds.do(job)

while True:
    schedule.run_pending()
    time.sleep(1)
