from multiprocessing.pool import ThreadPool

def getDBTables(db, withDBName=False):
  try:
    spark.sql(f"USE {db}")
    if withDBName:
      tables = [x['database']+'.'+x['tableName'] for x in spark.sql("SHOW TABLES").collect()]
    else:
      tables = [x['tableName'] for x in spark.sql("SHOW TABLES").collect()]
    print(f"Found {len(tables)} tables in {db}")
    return tables
  except Exception as e:
    print(e)
    print("Please fix the error and retry again.")

def publishTable_(table, db, folderPath):
  try:
    spark.sql(f'REFRESH TABLE {db}.{table}')
    file_name = folderPath+'/'+table
    temp_location = folderPath+'/'+table+'_inProgress'
    spark.sql(f"SELECT * FROM {db}.{table}").repartition(1).write.mode("overwrite").format("parquet").option("header", "true").option("nullValue", None).option("escape", '"').parquet(temp_location)
    print(table, db)
    for file_ in dbutils.fs.ls(temp_location):
      if file_.name[:4].lower()=='part':
        ## rename part file name to meaningful name
        old_file_name = temp_location+"/"+file_.name
        new_file_name = temp_location+"/"+table
        dbutils.fs.mv(old_file_name, new_file_name)
        ## now save file as single entity
        dbutils.fs.mv(new_file_name, file_name)
        dbutils.fs.rm(temp_location, True)
  except Exception as e:
    print(e)

def publishTables(tables, folderPath, db=None):
  '''
    This function takes 2 mandatory and 1 optional params
    Args:
        tables: List of tables
        folderPath: location for the tables dump
        db (by default it is None):
            If all the tables are from same db, pass db='DATABASE_NAME'
            else use two-part naming i.e., Database1.table1, database2.table3...

    Returns:
        This function doesn't return anything
  '''
  try:
    pool = ThreadPool(len(tables))
    if db:
      pool.map(lambda table: publishTable_(table, db, folderPath), tables)
    else:
      pool.map(lambda table: publishTable_(table.split('.')[-1], table.split('.')[0], folderPath), tables)  
  except Exception as e:
    print(e)
  finally:
    pool.close()
    pool.join()

def devCreds():
    return '''
        DatabaseUtils Code developed by: github.com/vinay26k
    '''
