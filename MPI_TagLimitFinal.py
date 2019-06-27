import numpy as np
from mpi4py import MPI
import time
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 2500
numberOfWorkers = 10
numberOfIter =100
numberOfpackets = (numberOfWorkers * 2) -1
numberOfComp = 3
pm = 0
p = 0.5
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
zeroCounter = 0
recvCounter = 0
flag=0



if rank == pm:
        ts = time.time()
        message = np.zeros(1)
        for i in range (1,numberOfIter+1):
            message[0] = i
            Y = np.empty([size, 1])
            while counter < numberOfpackets:
                comm.Recv(Y, source=MPI.ANY_SOURCE, tag=message[0], status=status)
                counter += 1
            for k in range (1,numberOfWorkers+1):
                req =comm.Send(message, dest=k)
            #print("message Send/bcasted")
            counter = 0
        tf = time.time()
        print(tf-ts)
else:
        message = np.zeros(1)
        compTag = 0
        sendTag = 0
        Y=np.zeros((size,1))
        req = comm.Irecv(message, source=pm)
        while recvCounter < numberOfIter:
            if MPI.Request.Test(req): # resets if message recv
                req = comm.Irecv(message, source=pm)
                recvCounter += 1
                counter = 0
                #print("message is ", message[0], "rank is", rank)
                #print("recvCounter is ", recvCounter, "rank is", rank)
            if (counter <numberOfComp  and recvCounter != numberOfIter):
                compTag = recvCounter + 1
                tet = np.random.rand(size, 1)
                Y = np.matmul(X, tet)
                sendTag = compTag
                #print("multed")
            if MPI.Request.Test(req):
                req = comm.Irecv(message, source=pm)
                recvCounter += 1
                sendTag = recvCounter+1
                counter = 0
                #print("message changed after the comp")
            if sendTag == compTag and counter < numberOfComp and recvCounter !=numberOfIter:
                comm.Send(Y, dest=pm, tag=sendTag)
                counter += 1
                #print("!!!SEND!!! message,rcounter,sendtag,compTagrank",message[0],recvCounter,sendTag,compTag,rank )
