import time, sys, cherrypy, os
from paste.translogger import TransLogger
from app import create_app
from pyspark import SparkContext, SparkConf

def init_spark_context():
    # load spark context
    conf = SparkConf().setAppName("book_recommendation-server")
    # IMPORTANT: pass aditional Python modules to each worker
    sc = SparkContext(conf=conf, pyFiles=['engine.py', 'app.py'])
 
    return sc
 
 
def run_server(app):
 
    # Enable WSGI access logging via Paste
    app_logged = TransLogger(app)
 
    # Mount the WSGI callable object (app) on the root directory
    cherrypy.tree.graft(app_logged, '/')
 
    # Set the configuration of the web server
    cherrypy.config.update({
        'engine.autoreload.on': True,
        'log.screen': True,
        'server.socket_port': 8000,
        'server.socket_host': '127.0.0.1'
    })
 
    # Start the CherryPy WSGI web server
    cherrypy.engine.start()
    cherrypy.engine.block()
 
 
if __name__ == "__main__":
    # Init spark context and load libraries
    sc = init_spark_context()
    # dataset_path = os.path.join('datasets', 'BX-CSV-Dump')
    hdfs_host = 'localhost'
    hdfs_port = '9000'

    # HDFS中的数据路径
    hdfs_path = '/user/user/datasets/BX-CSV-Dump'

    # 完整的HDFS路径
    dataset_path = f'hdfs://{hdfs_host}:{hdfs_port}{hdfs_path}'
    app = create_app(sc, dataset_path)
 
    # start web server
    run_server(app)

