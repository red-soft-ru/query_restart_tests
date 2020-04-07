from time import sleep
import fdb
import threading
from query_restart import *

prepare_db()

# blocker update the middle record
bc = qe('update t set v=-1 where i=3')

longThreads = []
for i in range(100):
    th = threading.Thread(target=qecc, args=(f'update t set v={i} where i=3',
                                                 ISOLATION_LEVEL_READ_COMMITED))
    longThreads.append(th)
    th.start()

sleep(2)    # wait when long thread hangs

# blocker commits transaction
bc.commit()
bc.close()

# stop unless the long thread finishes
for th in longThreads:
    th.join()

# check results
qecc('select * from t')
qecc('select tn, sn, o, n from log order by 1, 2, 4')
