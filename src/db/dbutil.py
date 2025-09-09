from db.dbsql import SessionLocal
from contextlib import contextmanager
debug=False
@contextmanager
def transaction():
    session = SessionLocal()
    try:
        # The 'yield' hands the session object to the 'with' block
        if(debug):
            print("DBUtil: Starting the transaction...")
        yield session
        # Execution is paused here until the 'with' block is finished
        if(debug):
            print("DBUtil: Triggering Commit")
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()