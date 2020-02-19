import os
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession

class PosgreConnector(object):

    def __init__(self, sqlContext):
        self.db = 'music_tiktok'
        self.hostname = os.environ['POSTGRES_HN']
        self.url = 'jdbc:postgresql://{}:5432/{}'.format(self.hostname, self.db)
        self.user = os.environ['POSTGRES_USR']
        self.password = os.environ['POSTGRES_PWD']
        self.driver = 'org.postgresql.Driver'
        self.properties = {'user': self.user, 'password':self.password,'driver':self.driver} 
        self.sqlContext = sqlContext

    def read_from_db(self, table_name):
    """
    read from the postgres db
    """
        return self.sqlContext.read.jdbc(url=self.url, table='public.{}'.format(table_name), properties=self.properties)

    def write_to_db(self, df, table_name, mode='append'):
    """
    write to the postgres db
    """
        df.write.jdbc(url=self.url, table='public.{}'.format(table_name), mode=mode, properties=self.properties)
