from time import sleep
import fdb
import threading
from query_restart import *

queries = (
    'delete from trig_v where v=-1 or i in (2, 3)',
    'update trig_v set v=1 where v=-1 or i in (2, 3)',
    'update vt set v=1 where v=-1 or i in (2, 3)',
    'delete from vt where v=-1 or i in (2, 3)',
    'execute procedure p_cur',
    'select * from p_s',
    'select * from p_slock',
    'execute procedure p_nos',
    'execute procedure p_upd',
    'select * from t where v=-1 or i in (3, 4) with lock',
    'select * from t where v=-1 or i in (2,3) with lock',
    '''
    merge into t dest
        using (select * from t where v=-1 or i in (2, 3)) src
        on dest.i=src.i
        when matched then
            update set dest.v=1
    ''',
    'update t set v=1 where v=-1 or i in (2, 3)',
    'delete from t where v=-1 or i in (2, 3)',
)

blocker_commit = (True, False)

iso_levels = (ISOLATION_LEVEL_READ_COMMITED,
              ISOLATION_LEVEL_SNAPSHOT,
              )

for iso_level in iso_levels:
    print(f'= ISOLATION LEVEL {iso_level}')

    for query in queries:
        print(f'== QUERY {query}')

        for b in blocker_commit:
            print(f'=== BLOCKER {b}')

            prepare_db()

            # blocker update the middle record
            bc = qe('update t set v=-1 where i=3')

            longThread = threading.Thread(target=qecc, args=(query, iso_level))
            longThread.start()
            sleep(2)    # wait when long thread hangs

            # 3th transaction update the first and the last records
            qecc('update t set v=-1 where i in (1, 5)')

            # blocker commits transaction
            tr = bc.transaction_info(isc_info_tra_id, 'i')
            if (b):
                bc.commit()
                print(f'T{tr} COMMITTED')
            else:
                bc.rollback()
                print(f'T{tr} ROLLED BACK')
            bc.close()

            # stop unless the long thread finishes
            longThread.join()

            # check results
            qecc('select * from t')
            qecc('select i, tn, sn, o, n from log')