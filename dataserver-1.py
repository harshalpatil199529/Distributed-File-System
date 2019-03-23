#!/usr/bin/env python
"""
Author: David Wolinsky
Version: 0.03
Description:
The XmlRpc API for this library is:
  get(base64 key)
    Returns the value associated with the given key using a dictionary
      or an empty dictionary if there is no matching key
    Example usage:
      rv = rpc.get(Binary("key"))
      print rv => Binary
      print rv.data => "value"
  put(base64 key, base64 value)
    Inserts the key / value pair into the hashtable, using the same key will
      over-write existing values
    Example usage:  rpc.put(Binary("key"), Binary("value"))
  print_content()
    Print the contents of the HT
  read_file(string filename)
    Store the contents of the Hahelperable into a file
  write_file(string filename)
    Load the contents of the file into the Hahelperable
Changelog:
    0.03 - Modified to remove timeout mechanism for data.
"""

import sys, SimpleXMLRPCServer, getopt, pickle, time, threading, xmlrpclib, unittest,shelve,os
from datetime import datetime, timedelta
from xmlrpclib import Binary
from collections import defaultdict
from sys import argv

xy=0            #for server no.
f=''            #for database name
# Presents a HT interface
class SimpleHT:
  def __init__(self):
    self.data = defaultdict(list)
    self.data1= defaultdict(list)
    self.r1data = defaultdict(list)
    self.r1data1= defaultdict(list)
    self.r2data = defaultdict(list)
    self.r2data1= defaultdict(list)
##############checksum dict initialisation###################################3
    self.cdata = defaultdict(list)
    self.cdata1= defaultdict(list)
    self.r1cdata = defaultdict(list)
    self.r1cdata1= defaultdict(list)
    self.r2cdata = defaultdict(list)
    self.r2cdata1= defaultdict(list)

 
  
  def checkpr(self):
    s=os.path.isfile(f)
    return Binary(pickle.dumps(s))



  def count(self):
    return len(self.data)
###########################################original##############################
  # Retrieve something from the HT
  def get(self, key):
    # Default return value
    s = shelve.open(f)
    self.data=s['data']
    rv = list
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.data:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.data[key]))

  # Insert something into the HT
  def put(self, key, value):
    s = shelve.open(f)
    value = pickle.loads(value.data)
    self.data[key.data] = value
    self.data1[key.data] = value
    s['data']=self.data
    s['data1']=self.data1
    s.close()
    print(".......................++++++++++++++++++++++++++++++++++++..........=================", self.data1)
    return Binary(pickle.dumps(True))


  # Retrieve something from the HT
  def get3(self):
    # Default return value
    s = shelve.open(f)
    self.data1=s['data1']
    rv = self.data1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def put3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.data1 = value
    self.data = value
    s['data']=self.data
    s['data1']=self.data1
    print(".......................++++++++++++++++++++++++++++++++++++..........=================", self.data1)
    s.close()
    return Binary(pickle.dumps(True))



###########################################original checksum##############################
  # Retrieve something from the HT
  def cget(self, key):
    # Default return value
    s = shelve.open(f)
    self.cdata=s['cdata']
    rv = list
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.cdata:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.cdata[key]))

  # Insert something into the HT
  def cput(self, key, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.cdata[key.data] = value
    self.cdata1[key.data] = value
    s['cdata']=self.cdata
    s['cdata1']=self.cdata1
    s.close()
    print("........................checksum og.........=================", self.cdata1)
    return Binary(pickle.dumps(True))


  # Retrieve something from the HT
  def cget3(self):
    # Default return value
    s = shelve.open(f)
    self.cdata1=s['cdata1']
    rv = self.cdata1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def cput3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.cdata1 = value
    self.cdata = value
    s['cdata']=self.cdata
    s['cdata1']=self.cdata1
    s.close()
    return Binary(pickle.dumps(True))





######################################################replica1###################################

  def r1get(self, key):
    # Default return value
    s = shelve.open(f)
    self.r1data=s['r1data']
    rv = list
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r1data:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.r1data[key]))

  # Insert something into the HT
  def r1put(self, key, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1data[key.data] = value
    self.r1data1[key.data] = value
    s['r1data']=self.r1data
    s['r1data1']=self.r1data1
    s.close()
    print("......................+++++++++++++++++++++++++++++++++=++++++++++++++++++++++++++++++++++++++++++++",self.r1data1)
    return Binary(pickle.dumps(True))



  def r1get3(self):
    s = shelve.open(f)
    # Default return value
    self.r1data1=s['r1data1']
    rv = self.r1data1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def r1put3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1data1 = value
    self.r1data = value
    s['r1data']=self.r1data
    s['r1data1']=self.r1data1
    print("......................+++++++++++++++++++++++++++++++++=++++++++++++++++++++++++++++++++++++++++++++",self.r1data1)
    s.close()
    return Binary(pickle.dumps(True))





######################################################replica1 checksum###################################

  def r1cget(self, key):
    # Default return value
    s = shelve.open(f)
    rv = list
    self.r1cdata=s['r1cdata']
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r1cdata:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.r1cdata[key]))

  # Insert something into the HT
  def r1cput(self, key, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1cdata[key.data] = value
    self.r1cdata1[key.data] = value
    s['r1cdata']=self.r1cdata
    s['r1cdata1']=self.r1cdata1
    s.close()
    print("......................++++++++++++++++++++checksum r1+++++++++++++=++++++++++++++++++++++++++++++++++++++++++++",self.r1cdata1)
    return Binary(pickle.dumps(True))



  def r1cget3(self):
    s = shelve.open(f)
    self.r1cdata1=s['r1cdata1']
    # Default return value
    rv = self.r1cdata1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def r1cput3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r1cdata1 = value
    self.r1cdata = value
    s['r1cdata']=self.r1cdata
    s['r1cdata1']=self.r1cdata1
    s.close()
    return Binary(pickle.dumps(True))




######################################################replica2###################################

  def r2get(self, key):
    s = shelve.open(f)
    self.r2data=s['r2data']
    # Default return value
    rv = list
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r2data:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.r2data[key]))

  # Insert something into the HT
  def r2put(self, key, value):
    
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2data[key.data] = value
    self.r2data1[key.data] = value
    s['r2data']=self.r2data
    s['r2data1']=self.r2data1
    s.close()
    print("......===============================================.............................................................",self.r2data1)
    return Binary(pickle.dumps(True))



  def r2get3(self):
    s = shelve.open(f)
    self.r2data1=s['r2data1']
    # Default return value
    rv = self.r2data1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def r2put3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2data1 = value
    self.r2data = value
    s['r2data']=self.r2data
    s['r2data1']=self.r2data1
    print("......===============================================.............................................................",self.r2data1)
    s.close()
    return Binary(pickle.dumps(True))




######################################################replica2 checksum###################################

  def r2cget(self, key):
    # Default return value
    s = shelve.open(f)
    self.r2cdata=s['r2cdata']
    rv = list
    # If the key is in the data structure, return properly formated results
    key = key.data
    print (key)
    if key in self.r2cdata:
        print("True")
    else:
        print("false")
    s.close()
    return Binary(pickle.dumps(self.r2cdata[key]))

  # Insert something into the HT
  def r2cput(self, key, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2cdata[key.data] = value
    self.r2cdata1[key.data] = value
    s['r2cdata']=self.r2cdata
    s['r2cdata1']=self.r2cdata1
    s.close()
    print("......==============================================checksum r2=.............................................................",self.r2cdata1)
    return Binary(pickle.dumps(True))



  def r2cget3(self):
    # Default return value
    s = shelve.open(f)
    self.r2cdata1=s['r2cdata1']
    rv = self.r2cdata1
    s.close()
    return Binary(pickle.dumps(rv))

  # Insert something into the HT
  def r2cput3(self, value):
    # Remove expired entries
    s=shelve.open(f)
    value = pickle.loads(value.data)
    self.r2cdata1 = value
    self.r2cdata = value
    s['r2cdata']=self.r2cdata
    s['r2cdata1']=self.r2cdata1
    s.close()
    return Binary(pickle.dumps(True))








################################################extra########################################################



  def remove(self, path):
    path = pickle.loads(path.data)
    del self.data[path]
    return Binary(pickle.dumps(True))
    
  # Load contents from a file
  def read_file(self, filename):
    f = open(filename.data, "rb")
    self.data = pickle.load(f)
    f.close()
    return True

  # Write contents to a file
  def write_file(self, filename):
    f = open(filename.data, "wb")
    pickle.dump(self.data, f)
    f.close()
    return True

  # Print the contents of the hashtable
  def print_content(self):
    print self.data
    return True

def main():
  global xy
  xy=int(argv[1])
  port = int(argv[xy+2])
  global f
  f='data store'+str(xy)
  serve(port)

# Start the xmlrpc server
def serve(port):
  file_server = SimpleXMLRPCServer.SimpleXMLRPCServer(('', port))
  file_server.register_introspection_functions()
  sht = SimpleHT()
  file_server.register_function(sht.get)
  file_server.register_function(sht.put)
  file_server.register_function(sht.get3)
  file_server.register_function(sht.put3)
  file_server.register_function(sht.r1get)
  file_server.register_function(sht.r1put)
  file_server.register_function(sht.r1get3)
  file_server.register_function(sht.r1put3)
  file_server.register_function(sht.r2get)
  file_server.register_function(sht.r2put)
  file_server.register_function(sht.r2get3)
  file_server.register_function(sht.r2put3)
  
  file_server.register_function(sht.cget)
  file_server.register_function(sht.cput)
  file_server.register_function(sht.cget3)
  file_server.register_function(sht.cput3)
  file_server.register_function(sht.r1cget)
  file_server.register_function(sht.r1cput)
  file_server.register_function(sht.r1cget3)
  file_server.register_function(sht.r1cput3)
  file_server.register_function(sht.r2cget)
  file_server.register_function(sht.r2cput)
  file_server.register_function(sht.r2cget3)
  file_server.register_function(sht.r2cput3)

  file_server.register_function(sht.checkpr)
  file_server.register_function(sht.remove)
  file_server.register_function(sht.print_content)
  file_server.register_function(sht.read_file)
  file_server.register_function(sht.write_file)
  file_server.serve_forever()

# Execute the xmlrpc in a thread ... needed for testing
class serve_thread:
  def __call__(self, port):
    serve(port)

# Wrapper functions so the tests don't need to be concerned about Binary blobs
class Helper:
  def __init__(self, caller):
    self.caller = caller

  def put(self, key, val, ttl):
    return self.caller.put(Binary(key), Binary(val), ttl)

  def get(self, key):
    return self.caller.get(Binary(key))

  def write_file(self, filename):
    return self.caller.write_file(Binary(filename))

  def read_file(self, filename):
    return self.caller.read_file(Binary(filename))

class SimpleHTTest(unittest.TestCase):
  def test_direct(self):
    helper = Helper(SimpleHT())
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

    helper.write_file("test")
    helper = Helper(SimpleHT())

    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    helper.read_file("test")
    self.assertEqual(helper.get("test")["value"], "test2", "Load unsuccessful!")
    self.assertTrue(helper.put("some_other_key", "some_value", 10000))
    self.assertEqual(helper.get("some_other_key")["value"], "some_value", "Different keys")
    self.assertEqual(helper.get("test")["value"], "test2", "Verify contents")

  # Test via RPC
  def test_xmlrpc(self):
    output_thread = threading.Thread(target=serve_thread(), args=(12345, ))
    output_thread.setDaemon(True)
    output_thread.start()

    time.sleep(1)
    helper = Helper(xmlrpclib.Server("http://127.0.0.1:51234"))
    self.assertEqual(helper.get("test"), {}, "DHT isn't empty")
    self.assertTrue(helper.put("test", "test", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test", "Failed to perform single get")
    self.assertTrue(helper.put("test", "test0", 10000), "Failed to put")
    self.assertEqual(helper.get("test")["value"], "test0", "Failed to perform overwrite")
    self.assertTrue(helper.put("test", "test1", 2), "Failed to put" )
    self.assertEqual(helper.get("test")["value"], "test1", "Failed to perform overwrite")
    time.sleep(2)
    self.assertEqual(helper.get("test"), {}, "Failed expire")
    self.assertTrue(helper.put("test", "test2", 20000))
    self.assertEqual(helper.get("test")["value"], "test2", "Store new value")

if __name__ == "__main__":
    main()
