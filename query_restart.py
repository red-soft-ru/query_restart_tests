import fdb
from fdb.ibase import *
from fdb.fbcore import *

fbClient = '/Library/Frameworks/Firebird.framework/Libraries/libfbclient.dylib'
database = '10.81.1.78:/home/roman/qr/qr.fdb'


def qe(query, traOpt = ISOLATION_LEVEL_READ_COMMITED):
    conn = fdb.connect(dsn=database, user='sysdba', password='masterkey', fb_library_name=fbClient)
    conn.begin(traOpt)
    cursor = conn.cursor()
    tr = conn.transaction_info(isc_info_tra_id, 'i')
    try:
        print(f'T{tr} EXECUTING: {query}')
        cursor.execute(query)

        if query.startswith('select'):
            res = []
            try:
                res = cursor.fetchall()

                print(f'T{tr} RESULT OF: {query}')
                for r in res:
                    print(r)

            except Exception as e:
                print(f'T{tr} FETCH ERROR: {e}')
        else:
            print(f'T{tr} EXECUTED: {query}')

    except Exception as e:
        print(f'T{tr} EXECUTE ERROR: {e}')

    return conn


def qec(query, traOpt = ISOLATION_LEVEL_READ_COMMITED):
    conn = qe(query, traOpt)
    tr = conn.transaction_info(isc_info_tra_id, 'i')
    conn.commit()
    print(f'T{tr} COMMITTED')
    return conn


def qecc(query, traOpt = ISOLATION_LEVEL_READ_COMMITED):
    qec(query, traOpt).close()


def prepare_db():
    try:
        conn = fdb.connect(dsn=database, user='sysdba', password='masterkey', fb_library_name=fbClient)
        conn.drop_database()

    except Exception as e:
        print(e)

    conn = fdb.create_database("create database '" + database + "' user 'sysdba' password 'masterkey'", fb_library_name=fbClient)
    cursor = conn.cursor()
    metadata = ("create table t (i int, v int)",
                "create table log (i int, tn int, sn int, o int, n int)",
                "insert into t values (1,0)",
                "insert into t values (2,0)",
                "insert into t values (3,0)",
                "insert into t values (4,0)",
                "insert into t values (5,0)",
                '''
                create trigger bu_t for t before update 
                as 
                    declare cn integer;
                    declare tn integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    in autonomous transaction do
                        insert into log values(old.i, :tn, :cn, old.v, new.v);
                end
                ''',
                '''
                create trigger bd_t for t before delete 
                as 
                    declare cn integer;
                    declare tn integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    in autonomous transaction do
                        insert into log values(old.i, :tn, :cn, old.v, null);
                end
                '''
                ,
                '''
                create procedure p_upd ()
                as
                    declare cn integer;
                    declare tn integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    in autonomous transaction do
                        insert into log values(null, -:tn, :cn, null, null);
                    update t set v=1 where v=-1 or i in (2, 3);
                end
                ''',
                '''
                create procedure p_s returns (v int)
                as
                    declare cn integer;
                    declare tn integer;
                    declare i integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    for select i, v from t where v=-1 or i in (2, 3) into :i, :v do
                    begin
                        in autonomous transaction do
                            insert into log values(:i, -:tn, :cn, :v, null);
                        suspend;
                    end
                end
                ''',
                '''
                create procedure p_slock returns (v int)
                as
                    declare cn integer;
                    declare tn integer;
                    declare i integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    for select i, v from t where v=-1 or i in (2, 3) with lock into :i, :v do
                    begin
                        in autonomous transaction do
                            insert into log values(:i, -:tn, :cn, :v, null);
                        suspend;
                    end
                end
                ''',
                '''
                create procedure p_nos
                as
                    declare cn integer;
                    declare tn integer;
                    declare i integer;
                    declare v integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    for select i, v from t where v=-1 or i in (2, 3) with lock into :i, :v do
                    begin
                        in autonomous transaction do
                            insert into log values(:i, -:tn, :cn, :v, null);
                    end
                end
                ''',
                '''
                create procedure p_cur
                as
                begin
                    for select * from t where v=-1 or i in (2, 3) as cursor cur do
                    begin
                        update t set v=1 where current of cur;
                    end
                end
                ''',
                'create view vt as select * from t',
                'create view trig_v as select distinct * from t where i>0',
                '''
                create trigger bu_trig_v for trig_v before update 
                as 
                    declare cn integer;
                    declare tn integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    in autonomous transaction do
                        insert into log values(old.i, -:tn, :cn, old.v, new.v);
                    update t set v=new.v where i=old.i;
                end
                ''',
                '''
                create trigger bd_trig_v for trig_v before delete 
                as 
                    declare cn integer;
                    declare tn integer;
                begin
                    cn = null; --rdb$get_context('SYSTEM', 'SNAPSHOT_NUMBER');
                    tn = CURRENT_TRANSACTION;
                    in autonomous transaction do
                        insert into log values(old.i, -:tn, :cn, old.v, null);
                    delete from t where i=old.i;
                end
                '''
                )

    for q in metadata:
        cursor.execute(q)
        conn.commit()

