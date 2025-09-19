from string.templatelib import Template, Interpolation

class Driver:
    def __init__(self, module):
        self.module = module

    def connect(self, *args, **kwargs):
        conn = self.module.connect(*args, **kwargs)
        return Connection(conn, self)

    @property
    def paramstyle(self):
        return self.module.paramstyle

class Connection:
    def __init__(self, db_api_conn, driver):
        self.db_api_conn = db_api_conn
        self.driver = driver

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        try:
            if exc_type:
                self.rollback()
            else:
                self.commit()
        finally:
            self.close()

    def cursor(self):
        cur = self.db_api_conn.cursor()
        return Cursor(cur, self.driver)
    
    def commit(self):
        return self.db_api_conn.commit()
    
    def rollback(self):
        return self.db_api_conn.rollback()

    def close(self):
        return self.db_api_conn.close()

class Cursor:
    def __init__(self, db_api_cursor, driver: Driver):
        self.db_api_cursor = db_api_cursor
        self.driver = driver
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
    
    def close(self):
        return self.db_api_cursor.close()
    
    def execute(self, query: Template):
        assert isinstance(query, Template)
        query_parts = []
        match self.driver.paramstyle:
            case "qmark" | "numeric" | "format":
                params = []
            case "named" | "pyformat":
                params = {}
        for i, item in enumerate(query, 1):
            match item:
                case str():
                    query_parts.append(item)
                case Interpolation():
                    match self.driver.paramstyle:
                        case "qmark":
                            query_parts.append('?')
                            params.append(item.value)
                        case "numeric":
                            query_parts.append(f":{i}")
                            params.append(item.value)
                        case "format":
                            query_parts.append(f"%s")
                            params.append(item.value)
                        case "named":
                            query_parts.append(f":PARAM_{i}")
                            params[f"PARAM_{i}"] = item.value
                        case "pyformat":
                            query_parts.append(f"%(param_{i})s")
                            params[f"param_{i}"] = item.value
        return self.db_api_cursor.execute(''.join(query_parts), params)

    @property
    def rowcount(self):
        return self.db_api_cursor.rowcount
    
    def fetchone(self):
        return self.db_api_cursor.fetchone()
    
    def fetchall(self):
        return self.db_api_cursor.fetchall()
    
    def fetchmany(self, count=None):
        if count is None:
            return self.db_api_cursor.fetchmany()
        else:
            return self.db_api_cursor.fetchmany(count)
