from db.dbsql import SessionLocal
from contextlib import contextmanager
@contextmanager
def transaction():
    session = SessionLocal()
    try:
        # The 'yield' hands the session object to the 'with' block
        print("DBUtil: Starting the transaction...")
        yield session
        # Execution is paused here until the 'with' block is finished
        print("DBUtil: Triggering Commit")
        session.commit()
    except Exception as e:
        session.rollback()
        raise
    finally:
        session.close()