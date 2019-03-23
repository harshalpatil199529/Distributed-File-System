# Distributed-File-System

Harshal Patil -55528581                                   

Shreenivas Pai N -78236469



Problem Statement

This project implements a distributed file system where in there is a client/server based architecture. It includes single robust remote meta server to store all the meta data of the file system and multiple data servers which stores the data using the concepts of distributed file system like redundancy and fault tolerance. Redundancy reduces the load by equally distributing the data among the other servers. In this project 2 replicas along with one original copy is stored on multiple data servers in round-robin fashion, which ensures that, if n-2 servers are alive the file system would still be able to retrieve the data correctly in proper order. In order to maintain the correctness and validity of the data an error correction method using checksum is implemented. Whenever the data is read, the file system 1st validates the checksum, if the checksum matches the data is fetched correctly. If the checksum fails to match the data is to be retrieved from one of the redundant replica which is stored in the next server in round-robin fashion. In case, if the 1st replica fails to match the checksum, 2nd replica is used to fetch the data along with the validation using checksum. Additionally the advantage of redundant distributed file system is that even if the server fails, client doesn’t have to wait until the failed server becomes active again to read the data. It still can fetch the data from the replicas stored on the other servers. The file system also implements a persistent storage wherein we create a file of all the data for every data server and if a server fails it can recover the complete state as of before the crash from its persistent storage file. The main database is in the disk and the file system always writes to the disk first before returning an RPC call. For implementing the persistent storage the python Shelve has benn used. Even when the persistent file fails and the server is crashed the file system can still retrieve the data from the replicas and create the persistent file and restore its state.
Design
This project implements a distributed file system where in there is a client/server based architecture. It includes single robust remote meta server to
store all the meta data of the file system and multiple data servers which stores the data using the concepts of distributed file system like redundancy and fault tolerance. Redundancy reduces the load by equally distributing the data among the other servers. In this project 2 replicas along with one original copy is stored on multiple data servers in round-robin fashion, which ensures that, if n-2 servers are alive the file system would still be able to retrieve the data correctly in proper order. In order to maintain the correctness and validity checksum is used by calculating its hash value.

This project consists of 3 main files

1. Metaserver.py
2. Dataserver.py
3. DistributedFS.py

Meta server consists of metadata of all files and directories. In this project we consider the meta server to robust and it never fails. It consists of two dicts. One for storing the meta files and another for storing the parent and contents of the directory.
Dataserver are multiple and project can support up to 5 dataservers assuming dataservers are not robust we created the redundant file system which keeps replicas of the original data block and it is stored in the next adjacent servers in round robin fashion. The dataserver consists of 3 dicts to store original, replica1 and replica2. Also, to maintain the validity of the data checksum is used. 3 dicts are used for storing checksum original, replica1 and replica 2. Each dataserver has persistent storage which stores all the data of corresponding server in disk. Whenever the dataserver fails, the data is retrieved from this storage. There is a chance that the persistence storage fails and that can be recovered from the redundant copies, and this will be done by the client. Shelve function is used to create the “data store”.
DistributedFS.py:
The client will support most of the functionalities that is supported by the Fuse file system. And some of them are explained below.

1. Write
When a write function is called the client will first check the availability of the server and if all the servers are alive it will proceed to write else it will wait until all the servers are alive. To check this functionality a separate function is written. When all the servers are alive it will get the
data from the respective server which is calculated using hash function. This hash function always will return a different value for different path. Write is done 8 bytes at a time and stored as list in the server. Same set of data is written in replica 1 and replica 2 in their respective servers. Alongside checksum is calculated and updated in respective server. Append is also included in write functionality. Separate function is used to do truncate. Following flowchart will give a flow of write operation.

2. Read
When a read function is called for a path, it will first check the availability on n-2 servers and if available it will proceed. It will calculate the hash for path and decide the server to read. If the calculated server is alive it will check for checksum. If the checksum is validated it will return. If it fails to validate the checksum or if the server is not alive it will go for the replicas of it and do the same.
3. Rename
Rename will start with checking of servers and if all the servers are available it will proceed. The hash value for both the old and new path is calculated and corresponding servers are found to be replaced. If move or rename is called corresponding functionality is performed. It will update the path in metserver and dataserver. When the path is updated in dataserver the server position is also updated i.e it is placed in the exact position where the hash value is calculated for the newpath.
4. Symlink symlink is also called soft link it is linked to the name of a file. When a symlink to a file is created anywhere the contents of the original files can be read properly. symlink can be moved from directory to directory without affecting the contents. symlink can also be deleted and it results in no changes to the original file.

Test cases passed 
1.Create a text file -1.txt 
2. Read the text file –cat 1.txt 
3.copy the data from 1.txt to 2.txt 
4.Read the copied file 2.txt 
5. Append to the copied file 
6. Read the file you just appended to 
7.rename the file 2.txt to 3.txt 
8.create a directory in root ‘a’ 
9. Go inside the directory and create a text file 4.txt 
10.move the the file 3.txt to the directory ‘a’ 
11.ls in the directory ‘a’ is showing 4.txt and 3.txt 
12.create a empty directory in root ‘b’ 
13.move the directory a to b 
14.go inside directory b and perform ls it shows 4.txt and 3.txt 
15.create another directory c in the root and create a file inside it. 
16.move the directoy ‘b’ to ‘c’ it shows the contents of both b and also contain the directory b. 
17.Rename the directory ‘c’ to ‘d’ 
18.delete an empty directory using rmdir it should get deleted. 
19.delete a directory with contents using rmdir it should give an error. 
20. delete a directory with contents using rm-rf should delete the directory.
21. create a symlink in the directory of a file that is in root. 
22. check for the source of the symlink 
23. move the symlink to any other directory. 
24. create a directory called ‘st_mode’ and check its stats. 
25. change the ownership of a directory 
26.change the ownership of a file 
27.change the permissions given to a file. 
28. truncate the text file should reduce the length of the file. 
29.truncate the text file to size that is larger than the file size the contents should remain the same. 
30.truncate the file size to 0 it should not show any contents now. 
31.crash any one server and try to read data it should be able to read the data. 
32. crash any 2 servers and try to read data it should be able to read the data. 
33.crash 3 servers and try to read it should wait until N-2 servers are alive. 
34.crash a server and write data it should wait for all the servers to alive. 
35.get all the server up and the write should automatically be completed. 
36.delete a persistent storage file and crash the server and bring it back up again it should be able to read normally and will remake the data store file. 
37.crash a server and delete the data store file and do the write operation it should wait for all the severs to be up. 
38. get the servers back up again and the write operation should be completed and the data store should be created by itself.

Conclusion

The Distributed file system has been successfully implemented. The filesystem can successfully perform all the basic operations like read, write, truncate, append, create, move, rename and others. The file system can support N=4 as well as N=5 data servers. Successfully able to store the data of the file on multiple data servers in round robin fashion. The system has also implemented checksum for ensuring the validity of the data. Even if up to 2 servers fail and crash at the same time the client is able to read all the data stored on the data servers, hence redundancy has been implemented successfully. The file system also implements a persistent storage wherein if any server crashes and after a certain period comes back alive the server is able to retrieve its complete state before resuming operation from the persistent storage. The file system is also capable of handling a case where the persistent storage fails. The filesystem is successfully able to check if the data received in the file is corrupt even when there is one data corruption per server in the same path in non adjacent server as well as corruption of multiple unique paths in different data servers. Once the file system knows that the data is corrupted it updates the data servers with the correct values from the replicas. Whenever a one or two servers are down the file system can do the read operation but blocks all write operation in case of server failures.
