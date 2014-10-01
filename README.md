This is a simple python ORM wrapper around MySQLdb, but it is not completed.
Now, it just supports the simple function of select,insert,update and delete.
I will update this project and make this ORM more powerful.


Requirements:
* Python 2.7
* MySQLdb
* MySQL


Structure:
* test.sql  the mysql sql schema
* model.py  the test ORM model
* db.py     the ORM code, it contains the test code in the end


Usage:

    import MySQLdb
    from MySQLdb.cursors import DictCursor
    from model import db_test
    db = DataBase(db_test,
        db='db_test',
        user='XXX',
        passwd='XXX',
        host='localhost',
        port=3306)
    # or db = DataBase(db_test, **config)
    print db.tb_test.insert({'id': 1,'name': 'insert'})
    print db.tb_test.where(id=1).update(name='up')
    print db.tb_test.where(id=1).select()
    print db.tb_test.where(id=1).delete()
