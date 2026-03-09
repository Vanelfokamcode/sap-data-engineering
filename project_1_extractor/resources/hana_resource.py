import os
from dagster import ConfigurableResource
from hdbcli import dbapi


class HanaCloudResource(ConfigurableResource):

    host: str
    port: int = 443
    user: str
    password: str

    def get_connection(self):
        return dbapi.connect(
            address=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            encrypt=True,
            sslValidateCertificate=False,
        )

    def execute(self, sql, params=None):
        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(sql, params or [])
        results = cursor.fetchall()
        conn.close()
        return results
