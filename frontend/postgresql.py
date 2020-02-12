import os
from pyspark import SparkContext, SparkConf
from pyspark.sql.session import SparkSession

class PosgreConnector(object):

    def __init__(self, sqlContext):
        self.df = df
        self.db = 'music_tiktok'
        self.url = 'jdbc:postgresql://ec2-34-197-195-174.compute-1.amazonaws.com:5432/%s' % self.db
        self.user = 'yvonneleoo'
        self.password = os.environ['POSGRES_PWD']
        self.driver = 'org.postgresql.Driver'
        self.properties = {'user': self.user, 'password':self.password,'driver':self.driver} 
        self.sqlContext = sqlContext

    def read_from_db(self, table_name):
        return self.sqlContext.read.jdbc(url=self.url, table='public.%s'%table_name, properties=self.properties)


    def write_to_db(self, df_clean, table_name, mode='append'):
        df_clean.write.jdbc(url=self.url, table='public.%s'%table_name, mode=mode, properties=self.properties)
