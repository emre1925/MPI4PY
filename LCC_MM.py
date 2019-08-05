import numpy as np
from mpi4py import MPI
import time
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 3000
numberOfWorkers = 20
numberOfIter =1000
numberOfpackets = (numberOfWorkers * 2) -1
numberOfComp = 3
iterIndex=1
pm = 0
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
flag=0
lastTagCounter = 0
totalrecv = 0
prevIterCounter=0
totalSent = 0
tsleep=0.006
p=0.2
def sendCheck(iterIndex, req):
    if iterIndex ==1:
        return True
    else:
        return MPI.Request.Test(req)
if rank == pm:
        ts = time.time()
        message = np.zeros(1)
        for i in range (1,numberOfIter+2):
            message[0] = i+1
            Y = np.empty([size, 1])
            if i == numberOfIter + 1:
                tf = time.time()
                print(tf - ts)
                print ("Total recv before the extra index ", totalrecv)
                while (lastTagCounter < numberOfWorkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    totalrecv += 1
                    if status.Get_tag() == i:
                        totalSent += int(Y[0][0])
                        lastTagCounter += 1
            else:
                #debugTag = []
                while counter < numberOfpackets:
                        comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                        totalrecv += 1
                        if status.Get_tag() == i:
                            counter += 1
                        else:
                            prevIterCounter += 1
                counter = 0
                if i%2==0:
                    for k in range (1,numberOfWorkers+1):
                         comm.Send(message, dest=k)
                else:
                    for k in range (numberOfWorkers,0,-1):
                         comm.Send(message, dest=k)

        print ("Total recv is ", totalrecv," Total send is ", totalSent," prevIterCounter is ",prevIterCounter)
else:
        message = np.zeros(1)
        compTag = 0
        sendCounter = 0
        Y=np.zeros((size,1))
        sleepFlag=1
        req = comm.Irecv(message, source=pm)
        req2 = None
        while iterIndex <= numberOfIter+1:
            if iterIndex == numberOfIter +1:
                Y[0][0]= sendCounter+1
                comm.Send(Y, dest=pm, tag=iterIndex)
                iterIndex += 1
                sendCounter += 1
            else:
                if sleepFlag !=1 :
                    if random.uniform(0, 1) < p:
                        time.sleep(tsleep)
                    sleepFlag = 1
                if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag: # resets if message recv
                    iterIndex += 1
                    counter = 0
                    sleepFlag = 0
                    if iterIndex <= numberOfIter:
                        req = comm.Irecv(message, source=pm)
                if (counter <numberOfComp) and iterIndex <= numberOfIter and sleepFlag:
                    compTag = iterIndex
                    tet = np.random.rand(size, 1)
                    Y = np.matmul(X, tet)
                if MPI.Request.Test(req)  and iterIndex <=numberOfIter and sleepFlag:
                    iterIndex += 1
                    counter = 0
                    sleepFlag = 0
                    if iterIndex <= numberOfIter:
                        req = comm.Irecv(message, source=pm)
                if iterIndex == compTag and counter < numberOfComp and iterIndex <= numberOfIter and sleepFlag and sendCheck(iterIndex, req2):
                    req2 = comm.Issend(Y, dest=pm, tag=iterIndex)
                    #req2.wait()
                    counter += 1
                    sendCounter +=1