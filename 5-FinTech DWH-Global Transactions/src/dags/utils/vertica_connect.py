from contextlib import contextmanager
from typing import Generator

import vertica_python
from airflow.hooks.base import BaseHook


class VerticaConnect:
    def __init__(self, host: str, port: str, database: str, user: str, password: str, 
                 autocommit: bool = True, connection_timeout: int = 30) -> None:
        self.host = host
        self.port = int(port)
        self.database = database
        self.user = user
        self.password = password
        self.autocommit = autocommit
        self.connection_timeout = connection_timeout

    def connection_info(self) -> dict:
        return {
            'host': self.host,
            'port': self.port,
            'database': self.database,
            'user': self.user,
            'password': self.password,
            'autocommit': self.autocommit,
            'connection_timeout': self.connection_timeout
        }

    def client(self):
        return vertica_python.connect(**self.connection_info())

    @contextmanager
    def connection(self) -> Generator[vertica_python.Connection, None, None]:
        conn = vertica_python.connect(**self.connection_info())
        try:
            yield conn
            if not self.autocommit:
                conn.commit()
        except Exception as e:
            if not self.autocommit:
                conn.rollback()
            raise e
        finally:
            conn.close()


class VerticaConnectionBuilder:

    @staticmethod
    def vertica_conn(conn_id: str) -> VerticaConnect:
        conn = BaseHook.get_connection(conn_id)

        autocommit = True
        connection_timeout = 30
        
        if "autocommit" in conn.extra_dejson:
            autocommit = conn.extra_dejson["autocommit"]
        if "connection_timeout" in conn.extra_dejson:
            connection_timeout = conn.extra_dejson["connection_timeout"]

        vertica = VerticaConnect(
            host=str(conn.host),
            port=str(conn.port),
            database=str(conn.schema),
            user=str(conn.login),
            password=str(conn.password),
            autocommit=autocommit,
            connection_timeout=connection_timeout
        )

        return vertica