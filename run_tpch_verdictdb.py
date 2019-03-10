import os,glob,subprocess,pyverdict,threading,time
from timeit import default_timer as timer
from multiprocessing import Process

## thread
def run_query(filename,sql):
    vc = pyverdict.impala('cp-2', 21050, 'tpch500g_parquet')
    vc.sql('use tpch500g_parquet')
    start = timer()
    vc.sql(text)
    end = timer()
    print('%s: %.4f seconds' % (filename, (end - start)))
    return

def run_thread(filename, sql, e):
    t = Process(target=run_query, args=(filename,sql,))
    t.start()
    while not e.isSet():
        if not t.is_alive():
            return
        time.sleep(1)
    if t.is_alive():
        t.terminate()
    t.join()
    return

maxtime=60
folder_path = './verdictdb_old'
for filename in glob.glob(os.path.join(folder_path, '*.sql')):
  #if "15.sql" not in filename and "11.sql" not in filename:
  #  continue
  with open(filename, 'r') as f:
    subprocess.call(['./flush_cache.sh'])
    text = f.read()
    text = text.replace('VERDICT_DATABASE', 'tpch500g_sample')
    e = threading.Event()
    t = threading.Thread(target=run_thread, args=(filename,text,e,))
    t.start()
    count=1;
    while t.is_alive():
        if (count > maxtime):
            print('%s timed out' % filename)
            e.set()
            break
        time.sleep(1)
        count=count+1
    t.join()
