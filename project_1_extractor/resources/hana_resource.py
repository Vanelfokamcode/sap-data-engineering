import os
from dagster import ConfigurableResource
from hdbcli import dbapi
from contextlib import contextmanager
class HanaCloudResource(ConfigurableResource):
 """Resource Dagster pour SAP HANA Cloud."""
 host: str
 port: int = 443
 user: str
 password: str
 def get_connection(self):
 """Retourne une connexion HANA active."""
 return dbapi.connect(
 address=self.host,
 port=self.port,
 user=self.user,
 password=self.password,
 encrypt=True,
 sslValidateCertificate=False,
 )
 def execute(self, sql: str, params=None):
 """Exécute une requête SQL et retourne les résultats."""
 conn = self.get_connection()
 cursor = conn.cursor()
 cursor.execute(sql, params or [])
 results = cursor.fetchall()
 conn.close()
 return results
