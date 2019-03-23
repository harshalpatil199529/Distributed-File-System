#!/usr/bin/env python

from __future__ import print_function, absolute_import, division
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import logging, xmlrpclib, pickle
from xmlrpclib import Binary
import os, hashlib,socket, time

from collections import defaultdict
from errno import ENOENT, ENOTEMPTY
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import sleep

if not hasattr(__builtins__, 'bytes'):
    bytes = str


def _get_rpc(sport):
    a = xmlrpclib.ServerProxy("http://localhost:"+str(int(sport)))

    try:
       	a._()   
    except xmlrpclib.Fault:
        # connected to the server 
        pass
    except socket.error:
        # Not connected 
        return False, None
	    	# Just in case the method is registered in the XmlRPC server
    return True, a


def check_server():
    global serv, scount
    serv=[None]*scount
    global ports,scount
    connected=[0]*scount
    stats=[0]*scount
    while 1:
        i=0
        sum1=0
        while i<scount:
            connected[i],serv[i]= _get_rpc(ports[i])
            i+=1
        i=0
        while i<scount:
            if connected[i]==True:
                print ("server"+str(i)+" is alive")
                stats[i]=1
            if connected[i]!=True:
                print("Waiting for server"+str(i))
            i+=1
        for every in stats:
            sum1+=every 
        if sum1==scount:
            print ("All servers are ready for contact")
	    if xyz==1:
		restore_per()
            break

xyz=0
def restore_per():
    global scount
    i=0
    while i<scount:
    	check=checkf(serv[i], i)
	if not check:
	    x=i
	    x1=(x+1)%scount
	    x2=(x-1)%scount
	    dat=r1getdata3(serv[x1], x1)
	    dat1=r2getdata3(serv[x1], x1)
	    dat2=r1getdata3(serv[x2], x2)
	    putdata3(serv[x], dat, x)
	    r1putdata3(serv[x], dat1, x)
	    r2putdata3(serv[x], dat2, x)
	    dat=cr1getdata3(serv[x1], x1)
	    dat1=cr2getdata3(serv[x1], x1)
	    dat2=cr1getdata3(serv[x2], x2)
	    cputdata3(serv[x], dat, x)
	    cr1putdata3(serv[x], dat1, x)
	    cr2putdata3(serv[x], dat2, x)
	i+=1
       

def check_server_read():
    global serv
    global ports,scount, xyz
    global serv_read
    global readcheckserv
    readcheckserv=[0]*scount 
    connected=[None]*scount
    while 1:
        i=0
        sum1=0
        while i<scount:
            connected[i],serv[i]= _get_rpc(ports[i])
            i+=1
        i=0
        while i<scount:
            if connected[i]==True:
                print ("server"+str(i)+" is alive")
                readcheckserv[i]=1
    		check=checkf(serv[i], i)
		if not check:
			restore_per()
            if connected[i]!=True:
                print("Waiting for server"+str(i))
            i+=1
        for every in readcheckserv:
            sum1+=every
	
        if sum1>=(scount-2):
            print ("atleast "+ str(sum1) +" servers are ready for contact")
            break
    return sum1

def checkf(server,x):
    return pickle.loads((serv[x].checkpr()).data)  

def putdata1(server,key,value):
    return server.put(Binary(key),Binary(pickle.dumps(value)))           

def getdata1(server,key):
    return pickle.loads((server.get(Binary(key))).data)


def putdata2(server,key,value,x):
    return serv[x].put(Binary(key),Binary(pickle.dumps(value)))          

def getdata2(server,key,x):
    return pickle.loads((serv[x].get(Binary(key))).data)


def getdata3(server,x):
    return pickle.loads((serv[x].get3()).data)                  

def putdata3(server,value,x):
    return serv[x].put3(Binary(pickle.dumps(value)))         

def r1putdata2(server,key,value,x):
    return serv[x].r1put(Binary(key),Binary(pickle.dumps(value)))          

def r1getdata2(server,key,x):
    return pickle.loads((serv[x].r1get(Binary(key))).data)

def r1getdata3(server,x):
    return pickle.loads((serv[x].r1get3()).data)                  

def r1putdata3(server,value,x):
    return serv[x].r1put3(Binary(pickle.dumps(value)))         

def r2putdata2(server,key,value,x):
    return serv[x].r2put(Binary(key),Binary(pickle.dumps(value)))          

def r2getdata2(server,key,x):
    return pickle.loads((serv[x].r2get(Binary(key))).data)

def r2getdata3(server,x):
    return pickle.loads((serv[x].r2get3()).data)                  

def r2putdata3(server,value,x):
    return serv[x].r2put3(Binary(pickle.dumps(value)))         

def cputdata2(server,key,value,x):
    return serv[x].cput(Binary(key),Binary(pickle.dumps(value)))          

def cgetdata2(server,key,x):
    return pickle.loads((serv[x].cget(Binary(key))).data)

def cgetdata3(server,x):
    return pickle.loads((serv[x].cget3()).data)                  

def cputdata3(server,value,x):
    return serv[x].cput3(Binary(pickle.dumps(value)))         

def cr1putdata2(server,key,value,x):
    return serv[x].r1cput(Binary(key),Binary(pickle.dumps(value)))          

def cr1getdata2(server,key,x):
    return pickle.loads((serv[x].r1cget(Binary(key))).data)

def cr1getdata3(server,x):
    return pickle.loads((serv[x].r1cget3()).data)                  

def cr1putdata3(server,value,x):
    return serv[x].r1cput3(Binary(pickle.dumps(value)))         

def cr2putdata2(server,key,value,x):
    return serv[x].r2cput(Binary(key),Binary(pickle.dumps(value)))          

def cr2getdata2(server,key,x):
    return pickle.loads((serv[x].r2cget(Binary(key))).data)

def cr2getdata3(server,x):
    return pickle.loads((serv[x].r2cget3()).data)                  

def cr2putdata3(server,value,x):
    return serv[x].r2cput3(Binary(pickle.dumps(value)))         


def corruptf(server,path,x):
    return serv[x].corrupt(Binary(pickle.dumps(path)))         
  

class Memory(LoggingMixIn, Operations):

    def stringtolist(self,s):
        L=[]
        while s!="":
            L.append(s[0:8])
            s=s[8:]
        return L


    def listtostring(self,l):
        s=''.join(l)
        return s
    
    def __init__(self):
        self.primary = {}        
        self.primary['files'] = {}
	self.primary['child'] = defaultdict(list)
	self.primary['pos'] = defaultdict(list)
        self.fd = 0
        now = time()
        self.primary['files']['/'] = dict(st_mode=(S_IFDIR | 0o755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        putdata1(server,"files",self.primary['files'])
        putdata1(server,"child",self.primary['child'])

    def chmod(self, path, mode):
        x = getdata1(server,"files")                      
        x[path]['st_mode'] &= 0o770000
        x[path]['st_mode'] |= mode
        putdata1(server,"files",x)
        return 0


    def chown(self, path, uid, gid):
        x = getdata1(server,"files")
        x[path]['st_uid'] = uid
        x[path]['st_gid'] = gid
        putdata1(server,"files",x)

    def create(self, path, mode):
	global serv, scount, xyz
        check_server()
	xyz=1
        fl1 = getdata1(server,"files")
        cl1 = getdata1(server,"child")
        fl1[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	i=0	
	while(i<scount):
	        putdata2(serv[i], path , [],i)
	        r1putdata2(serv[i], path , [],i)
	        r2putdata2(serv[i], path , [],i)
	        cputdata2(serv[i], path , [],i)
	        cr1putdata2(serv[i], path , [],i)
	        cr2putdata2(serv[i], path , [],i)
		i+=1
        parentpath=os.path.dirname(path)		
	childpath=os.path.basename(path)
        cl1[parentpath].append(childpath)            
        self.fd += 1
	putdata1(server,"files",fl1)
	putdata1(server,"child",cl1)
        return self.fd

    def getattr(self, path, fh=None):
        fl1 = getdata1(server,"files")
        if path not in fl1:
            raise FuseOSError(ENOENT)
        return fl1[path]

    def getxattr(self, path, name, position=0):
        fl1 = getdata1(server,"files")
        attrs = fl1[path].get('attrs', {})
        try:
            return attrs[name]
        except KeyError:
            return ''       

    def listxattr(self, path):
        fl1 = getdata1(server,"files")
        attrs = fl1[path].get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        fl1 = getdata1(server,"files")
        cl1 = getdata1(server,"child")
        fl1[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        parentpath=os.path.dirname(path)		
	childpath=os.path.basename(path)
        cl1[parentpath].append(childpath)                  
        cl1[path]=[]                             
        fl1[parentpath]['st_nlink'] += 1
        putdata1(server,"files",fl1)
        putdata1(server,"child",cl1)


    def open(self, path, flags):
        self.fd += 1
        return self.fd


    def read(self, path, size, offset, fh):
	global readcheckserv, scount
        sum1=check_server_read()
	if sum1>=(scount-2):
	    x=hash(path)%scount
	    x1=(x+1)%scount
	    x2=(x+2)%scount
            a1=path
            fl1 = getdata1(server,"files")
            leng= (fl1[path]['st_size'])
	    if leng%8 == 0:
	        leng=int(leng/8)
	    else:
	    	leng=int((leng/8)+1)
	    element=0
	    element_cond=0
	    dat=[]
	    data2=[]
	    while leng!=0:
		flag=0
		flag1=0
		flag2=0
		flag3=0
		if readcheckserv[x] == 1 and flag==0:
			checksum=cgetdata2(serv[x],path,x)
			echecksum=checksum[element]
			tocheck=getdata2(serv[x], path,x)
			etocheck=tocheck[element]
                        if echecksum == hash(etocheck):
	    			data2=etocheck
				flag=1
				flag1=1
		if readcheckserv[x1] == 1 and flag==0:
			r1checksum=cr1getdata2(serv[x1],path,x1)
			er1checksum=r1checksum[element]
			r1tocheck=r1getdata2(serv[x1], path,x1)
                        er1tocheck=r1tocheck[element]
			if er1checksum == hash(er1tocheck):
	    			data2=er1tocheck
				flag=1
				flag2=1
		if readcheckserv[x2] == 1 and flag==0:
			r2checksum=cr2getdata2(serv[x2],path,x2)
			er2checksum=r2checksum[element]
			r2tocheck=r2getdata2(serv[x2], path,x2)
			er2tocheck=r2tocheck[element]
			if er2checksum == hash(er2tocheck):
	    			data2=er2tocheck
				flag=1
				flag3=1

		if readcheckserv[x] == 1 and readcheckserv[x1] == 1:
			if flag1==0 and flag2==0 and flag3==1:
                                tocheck[element]=data2
			        checksum[element]=hash(data2)
                                r1tocheck[element]=data2
			        r1checksum[element]=hash(data2)
				cputdata2(serv[x],path,checksum,x)
				cr1putdata2(serv[x1],path,r1checksum,x1)
				putdata2(serv[x],path,data2,x)
				r1putdata2(serv[x1],path,data2,x1)
			if flag1==0 and flag2==1:
                                tocheck[element]=data2
			        checksum[element]=hash(data2)
				cputdata2(serv[x],path,checksum,x)
				putdata2(serv[x],path,tocheck,x)
		if readcheckserv[x] == 1 and readcheckserv[x1] == 0:
			if flag1==0 and flag3==1:
				tocheck[element]=data2
			        checksum[element]=hash(data2)
				cputdata2(serv[x],path,checksum,x)
				putdata2(serv[x],path,tocheck,x)
		if readcheckserv[x] == 0 and readcheckserv[x1] == 1:
			if flag2==0 and flag3==1:
                                r1tocheck[element]=data2
			        r1checksum[element]=hash(data2)
				cr1putdata2(serv[x1],path,r1checksum,x1)
				r1putdata2(serv[x1],path,r1tocheck,x1)
	  

	    	dat.append(data2)
	    	element_cond=(element_cond+1)%scount
	    	if element_cond == 0:
	    		element+=1
	        x=(x+1)%scount
		x1=(x1+1)%scount
		x2=(x2+1)%scount
	    	leng-=1
            s1 = dat[int(offset/8):int(((offset+size)/8) + 1)]    
            s1 = self.listtostring(s1)
            s1 = s1[offset%8 : ] + s1[-((offset+size)%8):]
            return  s1                                            


    def readdir(self, path, fh):
        cl1 = getdata1(server,"child")
        parentpath=os.path.dirname(path)		
	childpath=os.path.basename(path)    
        for x in cl1: 
            if x==path: 
                return ['.', '..']+cl1[path]     

    def readlink(self, path):
	global readcheckserv, scount
        sum1=check_server_read()
	if sum1>=(scount-2):
	    x=hash(path)%scount
	    x1=(x+1)%scount
	    x2=(x+2)%scount
            dat = getdata2(serv[x], path,x)
        return self.listtostring(dat)

    def removexattr(self, path, name):
        fl1 = getdata1(server,"files")
        attrs = fl1[path].get('attrs', {})
        try:
            del attrs[name]
            putdata1(server,"files",fl1)
        except KeyError:
            pass        



    def rename(self, old, new):
	global serv, scount
        check_server()
        fl1 = getdata1(server,"files")
        cl1 = getdata1(server,"child")
	x = hash(old)%scount
	x1 = (x+1)%scount
	x2 = (x+2)%scount
	y = hash(new)%scount
	y1 = (y+1)%scount
	y2 = (y+2)%scount
	dat = getdata3(serv[x],x)
	parentpathold=os.path.dirname(old)		
	childpathold=os.path.basename(old)
	parentpathnew=os.path.dirname(new)
	childpathnew=os.path.basename(new)
	if childpathold == childpathnew:
	   	cl1[parentpathnew].append(childpathnew)
	   	cl1[parentpathold].remove(childpathold)
	    	for i in cl1.keys():
			listnew=i.split('/')		
			if childpathold in listnew:
				a_po=i.find(childpathold)
				attach=i[a_po:]
				newkey=parentpathnew + '/' + attach
				cl1[newkey]=cl1.pop(i)
				fl1[newkey]=fl1.pop(i)


		for i in dat.keys():
			listnew=i.split('/')
			if childpathold in listnew:
				a_po=i.find(childpathold)
				attach=i[a_po:]
		    		newkey=parentpathnew + '/' + attach
		   		fl1[newkey]=fl1.pop(i)
				n=0
				x = hash(i)%scount
				x1 = (x+1)%scount
				x2 = (x+2)%scount
				y = hash(newkey)%scount
				y1 = (y+1)%scount
				y2 = (y+2)%scount	
				while(n<scount):
					temp=[]
					temp1=[]
					temp2=[]
					temp3=[]
					temp4=[]
					temp5=[]
	        			dat = getdata3(serv[x], x)
	        			dat1 = r1getdata3(serv[x1], x1)
	        			dat2 = r2getdata3(serv[x2], x2)
	        			dat3 = cgetdata3(serv[x],x)
	        			dat4 = cr1getdata3(serv[x1],x1)
	        			dat5 = cr2getdata3(serv[x2],x2)
					temp = dat[i]
					temp1 = dat1[i]
					temp2 = dat2[i]
					temp3 = dat3[i]
					temp4 = dat4[i]
					temp5 = dat5[i]
					dat.pop(i)
					dat1.pop(i)
					dat2.pop(i)
					dat3.pop(i)
					dat4.pop(i)
					dat5.pop(i)
					putdata3(serv[x], dat,x)
	        			r1putdata3(serv[x1], dat1,x1)
	        			r2putdata3(serv[x2], dat2,x2)
	        			cputdata3(serv[x], dat3,x)
	        			cr1putdata3(serv[x1], dat4,x1)
	        			cr2putdata3(serv[x2], dat5,x2)

	        			dat = getdata3(serv[y], y)
	        			dat1 = r1getdata3(serv[y1], y1)
	        			dat2 = r2getdata3(serv[y2], y2)
	        			dat3 = cgetdata3(serv[y],y)
	        			dat4 = cr1getdata3(serv[y1],y1)
	        			dat5 = cr2getdata3(serv[y2],y2)
					dat[newkey]=temp
					dat1[newkey]=temp1
					dat2[newkey]=temp2
					dat3[newkey]=temp3
					dat4[newkey]=temp4
					dat5[newkey]=temp5
					putdata3(serv[y], dat,y)
	        			r1putdata3(serv[y1], dat1,y1)
	        			r2putdata3(serv[y2], dat2,y2)
	        			cputdata3(serv[y], dat3,y)
	        			cr1putdata3(serv[y1], dat4,y1)
	        			cr2putdata3(serv[y2], dat5,y2)
					n+=1
					x=(x+1)%scount
					x1=(x1+1)%scount
					x2=(x2+1)%scount
					y=(y+1)%scount
					y1=(y1+1)%scount
					y2=(y2+1)%scount					

		
	else:
		for i in dat.keys():		
			oldkey=i
			listnew=i.split('/')
			for n,k in enumerate(listnew):
				if k==childpathold:
					listnew[n]=childpathnew                                                         
       					newkey = "/".join(listnew)
					fl1[newkey] =fl1.pop(oldkey)
					n=0
					x = hash(oldkey)%scount
					x1 = (x+1)%scount
					x2 = (x+2)%scount
					y = hash(newkey)%scount
					y1 = (y+1)%scount
					y2 = (y+2)%scount	
					while(n<scount):
						temp=[]
						temp1=[]
						temp2=[]
						temp3=[]
						temp4=[]
						temp5=[]
	        				dat = getdata3(serv[x], x)
	        				dat1 = r1getdata3(serv[x1], x1)
	        				dat2 = r2getdata3(serv[x2], x2)
	        				dat3 = cgetdata3(serv[x],x)
	        				dat4 = cr1getdata3(serv[x1],x1)
	        				dat5 = cr2getdata3(serv[x2],x2)
						temp = dat[oldkey]
						temp1 = dat1[oldkey]
						temp2 = dat2[oldkey]
						temp3 = dat3[oldkey]
						temp4 = dat4[oldkey]
						temp5 = dat5[oldkey]
						dat.pop(oldkey)
						dat1.pop(oldkey)
						dat2.pop(oldkey)
						dat3.pop(oldkey)
						dat4.pop(oldkey)
						dat5.pop(oldkey)
						putdata3(serv[x], dat,x)
	        				r1putdata3(serv[x1], dat1,x1)
	        				r2putdata3(serv[x2], dat2,x2)
	        				cputdata3(serv[x], dat3,x)
	        				cr1putdata3(serv[x1], dat4,x1)
	        				cr2putdata3(serv[x2], dat5,x2)
	
	        				dat = getdata3(serv[y], y)
	        				dat1 = r1getdata3(serv[y1], y1)
	        				dat2 = r2getdata3(serv[y2], y2)
	        				dat3 = cgetdata3(serv[y],y)
	        				dat4 = cr1getdata3(serv[y1],y1)
	        				dat5 = cr2getdata3(serv[y2],y2)
						dat[newkey]=temp
						dat1[newkey]=temp1
						dat2[newkey]=temp2
						dat3[newkey]=temp3
						dat4[newkey]=temp4
						dat5[newkey]=temp5	
						putdata3(serv[y], dat,y)
	        				r1putdata3(serv[y1], dat1,y1)
	        				r2putdata3(serv[y2], dat2,y2)
	        				cputdata3(serv[y], dat3,y)
	        				cr1putdata3(serv[y1], dat4,y1)
	        				cr2putdata3(serv[y2], dat5,y2)
						n+=1
						x=(x+1)%scount
						x1=(x1+1)%scount
						x2=(x2+1)%scount
						y=(y+1)%scount
						y1=(y1+1)%scount
						y2=(y2+1)%scount

		for i in cl1.keys():		
			oldkey=i
			listnew=i.split('/')
			for n,k in enumerate(listnew):
				if k==childpathold:
					listnew[n]=childpathnew                                                         
       					newkey = "/".join(listnew)
					cl1[newkey]=cl1.pop(oldkey)
					fl1[newkey] =fl1.pop(oldkey)

		parentnon= os.path.dirname(new)
		cl1[parentnon].append(childpathnew)			        
		cl1[parentnon].remove(childpathold)

	putdata1(server,"files",fl1)
        putdata1(server,"child",cl1)


    def rmdir(self, path):
	global serv, scount
        check_server() 
        fl1 = getdata1(server,"files")
        cl1 = getdata1(server,"child")
	if fl1[path]['st_nlink'] <= 2:
            parentpath=os.path.dirname(path)		
	    childpath=os.path.basename(path)
	    fl1.pop(path)
            cl1.pop(path)
            cl1[parentpath].remove(childpath)
            fl1[parentpath]['st_nlink'] -= 1
            putdata1(server,"files",fl1)
            putdata1(server,"child",cl1)
	else:
	    raise FuseOSError(ENOTEMPTY)	
		
    def setxattr(self, path, name, value, options, position=0):
        fl1 = getdata1(server,"files")
        attrs = fl1[path].setdefault('attrs', {})
        attrs[name] = value
        putdata1(server,"files",fl1)


    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
	global serv, scount
        check_server() 
        fl1 = getdata1(server,"files")
	cl1 = getdata1(server,"child")	
        fl1[target] = dict(st_mode=(S_IFLNK | 0o777), st_nlink=1, st_size=len(source))
	parentpath=os.path.dirname(target)		
	childpath=os.path.basename(target)	
	cl1[parentpath].append(childpath)
        putdata1(server,"files",fl1)
	putdata1(server,"child",cl1)
	x=hash(target)%scount
	x1=(x+1)%scount
	x2=(x+2)%scount
	dat = getdata2(serv[x], target,x)
	dat = self.stringtolist(source)
        putdata2(serv[x], target, dat,x)
        r1putdata2(serv[x1], target, dat,x1)
        r2putdata2(serv[x2], target, dat,x2)
	cdat=hash(source)
        cputdata2(serv[x], target, dat,x)
        cr1putdata2(serv[x1], target, dat,x1)
        cr2putdata2(serv[x2], target, dat,x2)


    def truncate(self, path, length, fh=None): 
	global serv, scount
        check_server() 
	x=hash(path)%scount
	x1=(x+1)%scount
	x2=(x+2)%scount
        fl1 = getdata1(server,"files") 
        leng= (fl1[path]['st_size'])
	if length > leng:
            fl1[path]['st_size'] = leng
            putdata1(server,"files",fl1)
	else:
	    if leng%8 == 0:
	        leng=int(leng/8)
	    else:
	    	leng=int((leng/8)+1)
	    element=0
	    element_cond=0
	    dat=[]
	    r1dat=[]
	    r2dat=[]
	    while leng!=0:
	    	data2=getdata2(serv[x], path,x)
	    	r1data2=r1getdata2(serv[x1], path,x1)
	    	r2data2=r2getdata2(serv[x2], path,x2)
	    	dat.append(data2[element])
	    	r1dat.append(r1data2[element])
	    	r2dat.append(r2data2[element])
	    	element_cond=(element_cond+1)%scount
	    	if element_cond == 0:
	    		element+=1
	    	x=(x+1)%scount
	    	x1=(x1+1)%scount
	    	x2=(x2+1)%scount
	    	leng-=1
            s1 = dat
            s1 = self.listtostring(s1)
	    s1 = s1[0:length]
            r1s1 = r1dat
            r1s1 = self.listtostring(r1s1)
	    r1s1 = r1s1[0:length]
            r2s1 = r2dat
            r2s1 = self.listtostring(r2s1)
	    r2s1 = r2s1[0:length]	 
	    i=0
            x=hash(path)%scount
            x1=(x+1)%scount
            x2=(x+2)%scount
	    while i!=scount:
	    	dat = getdata3(serv[x],x)
		cdat = cgetdata3(serv[x],x)
	    	if path not in dat.keys():
	    		i+=1
                        x=(x+1)%scount
			continue
             	dat.pop(path)
	    	putdata3(serv[x], dat,x)
             	cdat.pop(path)
	    	cputdata3(serv[x], cdat,x)
	    	i+=1
	    	x=(x+1)%scount
	    i=0
	    while i!=scount:
	    	r1dat = r1getdata3(serv[x1],x1)
	    	cr1dat = cr1getdata3(serv[x1],x1)
	    	if path not in r1dat.keys():
	    		i+=1
                        x1=(x1+1)%scount
			continue
            	r1dat.pop(path)
	    	r1putdata3(serv[x1], r1dat,x1)
            	cr1dat.pop(path)
	    	cr1putdata3(serv[x1], cr1dat,x1)
	    	i+=1
	    	x1=(x1+1)%scount
	    i=0
	    while i!=scount:
	    	r2dat = r2getdata3(serv[x2],x2)
	    	cr2dat = cr2getdata3(serv[x2],x2)
	    	if path not in r2dat.keys():
	    		i+=1
                        x2=(x2+1)%scount
			continue
            	r2dat.pop(path)
	    	r2putdata3(serv[x2], r2dat,x2)
            	cr2dat.pop(path)
	    	cr2putdata3(serv[x2], cr2dat,x2)
	    	i+=1
	    	x2=(x2+1)%scount
	    leng=len(s1)
	    if leng%8 == 0:
	    	leng=int(leng/8)
	    else:
	    	leng=int((leng/8)+1)
	    data1=self.stringtolist(s1)
            x=hash(path)%scount
	    i=0
	    while leng!=0:
	    	dat = getdata2(serv[x],path,x)
            	dat.append(data1[i])
	    	putdata2(serv[x],path,dat,x)
	    	cdat = cgetdata2(serv[x],path,x)
            	cdat.append(hash(data1[i]))
	    	cputdata2(serv[x],path,cdat,x)
	    	x=(x+1)%scount
	    	i+=1
	    	leng-=1

	    r1leng=len(r1s1)
	    if r1leng%8 == 0:
	    	r1leng=int(r1leng/8)
	    else:
	    	r1leng=int((r1leng/8)+1)
	    r1data1=self.stringtolist(r1s1)
            x=hash(path)%scount
	    x1=(x+1)%scount
	    i=0
	    while r1leng!=0:
	    	r1dat = r1getdata2(serv[x1],path,x1)
            	r1dat.append(r1data1[i])
	    	r1putdata2(serv[x1],path,r1dat,x1)
	    	cr1dat = cr1getdata2(serv[x1],path,x1)
            	cr1dat.append(hash(r1data1[i]))
	    	cr1putdata2(serv[x1],path,cr1dat,x1)
	    	x1=(x1+1)%scount
	    	i+=1
	    	r1leng-=1


	    r2leng=len(r2s1)
	    if r2leng%8 == 0:
	    	r2leng=int(r2leng/8)
	    else:
	    	r2leng=int((r2leng/8)+1)
	    r2data1=self.stringtolist(r2s1)
            x=hash(path)%scount
	    x2=(x+2)%scount
	    i=0
	    while r2leng!=0:
	    	r2dat = r2getdata2(serv[x2],path,x2)
            	r2dat.append(r2data1[i])
	    	r2putdata2(serv[x2],path,r2dat,x2)
	    	cr2dat = cr2getdata2(serv[x2],path,x2)
            	cr2dat.append(hash(r2data1[i]))
	    	cr2putdata2(serv[x2],path,cr2dat,x2)
	    	x2=(x2+1)%scount
	    	i+=1
	    	r2leng-=1
	    
            fl1[path]['st_size'] = length
            putdata1(server,"files",fl1)

   
    def unlink(self, path):
	global serv, scount
        check_server() 
        fl1 = getdata1(server,"files")
        cl1 = getdata1(server,"child")
	fl1.pop(path)
        parentpath=os.path.dirname(path)		
	childpath=os.path.basename(path)
        cl1[parentpath].remove(childpath)
        putdata1(server,"files",fl1)
        putdata1(server,"child",cl1)
        x=hash(path)%scount
	x1=(x+1)%scount
	x2=(x+2)%scount
	i=0
	while i!=scount:
	    dat = getdata3(serv[x],x)
	    r1dat = r1getdata3(serv[x1],x1)
	    r2dat = r2getdata3(serv[x2],x2)
            dat.pop(path)
            r1dat.pop(path)
            r2dat.pop(path)
	    putdata3(serv[x], dat,x)
	    r1putdata3(serv[x1], r1dat,x1)
	    r2putdata3(serv[x2], r2dat,x2)
	    cdat = cgetdata3(serv[x],x)
	    cr1dat = cr1getdata3(serv[x1],x1)
	    cr2dat = cr2getdata3(serv[x2],x2)
            cdat.pop(path)
            cr1dat.pop(path)
            cr2dat.pop(path)
	    cputdata3(serv[x], cdat,x)
	    cr1putdata3(serv[x1], cr1dat,x1)
	    cr2putdata3(serv[x2], cr2dat,x2)
	    i+=1
	    x=(x+1)%scount
	    x1=(x1+1)%scount
	    x2=(x2+1)%scount
	

    def utimens(self, path, times=None):
        fl1 = getdata1(server,"files")
        now = time()
        atime, mtime = times if times else (now, now)
        fl1[path]['st_atime'] = atime
        fl1[path]['st_mtime'] = mtime
        putdata1(server,"files",fl1)

    def write(self, path, data, offset, fh): 
	global serv, scount
        check_server() 
        fl1 = getdata1(server,"files") 
        leng=fl1[path]['st_size']
	if leng%8 == 0:
	    leng=int(leng/8)
	else:
	    leng=int((leng/8)+1)

        if leng==0: 
            data1=self.stringtolist(data)
	    x=hash(path)%scount
	    x1=(x+1)%scount
	    x2=(x+2)%scount
	    length = len(data1)
	    i=0
	    while length!=0:
		dat = getdata2(serv[x],path,x)
		r1dat = r1getdata2(serv[x1],path,x1)
		r2dat = r2getdata2(serv[x2],path,x2)		
		dat.append(data1[i])
		r1dat.append(data1[i])
		r2dat.append(data1[i])
		putdata2(serv[x],path,dat,x)
		r1putdata2(serv[x1],path,r1dat,x1)
		r2putdata2(serv[x2],path,r2dat,x2)
		cs = cgetdata2(serv[x],path,x)
		r1cs = cr1getdata2(serv[x1],path,x1)
		r2cs = cr2getdata2(serv[x2],path,x2)
		cs.append(hash(data1[i]))
		r1cs.append(hash(data1[i]))
		r2cs.append(hash(data1[i]))
		cputdata2(serv[x],path,cs,x)
		cr1putdata2(serv[x1],path,r1cs,x1)
		cr2putdata2(serv[x2],path,r2cs,x2)
		x=(x+1)%scount
		x1=(x1+1)%scount
		x2=(x2+1)%scount
		i+=1
		length-=1          

        else:    
            x=hash(path)%scount
	    x1=(x+1)%scount
	    x2=(x+2)%scount
	    data1=self.stringtolist(data)
	    y=((leng+x)%scount)
	    y1=((leng+x1)%scount)
	    y2=((leng+x2)%scount)
	    ll=getdata2(serv[y-1],path,(y-1))
	    ll1=r1getdata2(serv[y1-1],path,(y1-1))
	    ll2=r2getdata2(serv[y2-1],path,(y2-1))
	    cll=cgetdata2(serv[y-1],path,(y-1))
	    cll1=cr1getdata2(serv[y1-1],path,(y1-1))
	    cll2=cr2getdata2(serv[y2-1],path,(y2-1))
            if len(ll[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			dat = getdata2(serv[y],path,y)
			dat.append(data1[i])
			putdata2(serv[y],path,dat,y)
			cdat = cgetdata2(serv[y],path,y)
			cdat.append(hash(data1[i]))
			cputdata2(serv[y],path,cdat,y)
			y=(y+1)%scount
			i+=1
			length-=1
            if len(ll1[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			r1dat = r1getdata2(serv[y1],path,y1)
			r1dat.append(data1[i])
			r1putdata2(serv[y1],path,r1dat,y1)
			cr1dat = cr1getdata2(serv[y1],path,y1)
			cr1dat.append(hash(data1[i]))
			cr1putdata2(serv[y1],path,cr1dat,y1)
			y1=(y1+1)%scount
			i+=1
			length-=1
            if len(ll2[-1]) == 8:
                i=0
	        length = len(data1)
	    	while length!=0:
			r2dat = r2getdata2(serv[y2],path,y2)
			r2dat.append(data1[i])
			r2putdata2(serv[y2],path,r2dat,y2)
			cr2dat = cr2getdata2(serv[y2],path,y2)
			cr2dat.append(hash(data1[i]))
			cr2putdata2(serv[y2],path,cr2dat,y2)
			y2=(y2+1)%scount
			i+=1
			length-=1

            if len(ll[-1])!=8:
                i=0
		y=y-1
		data1 = self.stringtolist(ll[-1] + data)
		del ll[-1]
		putdata2(serv[y],path,ll,y)
		del cll[-1]
		cputdata2(serv[y],path,cll,y)
		length=len(data1)
	    	while length!=0:
			dat = getdata2(serv[y],path,y)
			dat.append(data1[i])
			putdata2(serv[y],path,dat,y)
			cdat = cgetdata2(serv[y],path,y)
			cdat.append(hash(data1[i]))
			cputdata2(serv[y],path,cdat,y)
			y=(y+1)%scount
			i+=1
			length-=1

            if len(ll1[-1])!=8:
                i=0
		y1=y1-1
		data1 = self.stringtolist(ll1[-1] + data)
		del ll1[-1]
		r1putdata2(serv[y1],path,ll1,y1)
		del cll1[-1]
		cr1putdata2(serv[y1],path,cll1,y1)
		length=len(data1)
	    	while length!=0:
			r1dat = r1getdata2(serv[y1],path,y1)
			r1dat.append(data1[i])
			r1putdata2(serv[y1],path,r1dat,y1)
			cr1dat = cr1getdata2(serv[y1],path,y1)
			cr1dat.append(hash(data1[i]))
			cr1putdata2(serv[y1],path,cr1dat,y1)
			y1=(y1+1)%scount
			i+=1
			length-=1

            if len(ll2[-1])!=8:
                i=0
		y2=y2-1
		data1 = self.stringtolist(ll2[-1] + data)
		del ll2[-1]
		r2putdata2(serv[y2],path,ll2,y2)
		del cll2[-1]
		cr2putdata2(serv[y2],path,cll2,y2)
		length=len(data1)
	    	while length!=0:
			r2dat = r2getdata2(serv[y2],path,y2)
			r2dat.append(data1[i])
			r2putdata2(serv[y2],path,r2dat,y2)
			cr2dat = cr2getdata2(serv[y2],path,y2)
			cr2dat.append(hash(data1[i]))
			cr2putdata2(serv[y2],path,cr2dat,y2)
			y2=(y2+1)%scount
			i+=1
			length-=1

        fl1[path]['st_size'] += len(data)              
        putdata1(server,"files",fl1)
        return len(data)

if __name__ == '__main__':
    mport=argv[2]
    global scount
    scount = (len(argv)-3)
    global ports
    ports=[]
    i=0
    while i<scount:
        ports.append(int(argv[i+3]))
        i+=1
    print (ports)
    server = xmlrpclib.ServerProxy("http://localhost:"+str(int(mport)))
    if len(argv) > 8:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.basicConfig(level=logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
