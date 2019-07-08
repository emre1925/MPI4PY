import numpy as np
from mpi4py import MPI
import time
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 3000
numberOfWorkers = 10
numberOfIter = 1000
treshold = 8
prevRecvCounter =0
numberOfComp = 3
pm = 0
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
lastTagCounter= 0
tsleep=0.018
p=0.2



if rank == pm:
        ts = time.time()
        message = np.zeros(1)
        for i in range (1,numberOfIter+2):
            message[0] = i+1
            Y = np.empty([size, 1])
            if i == numberOfIter+1:
                tf = time.time()
                print(tf - ts)
                while(lastTagCounter < numberOfWorkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag()==i:
                       lastTagCounter += 1
                    else:
                        prevRecvCounter +=1
            else:
                while counter < treshold:
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag() == i:
                        counter += 1
                    else:
                        prevRecvCounter +=1
                counter = 0
                if i % 2 == 0:
                    for k in range(1, numberOfWorkers + 1):
                        comm.Send(message, dest=k)
                else:
                    for k in range(numberOfWorkers, 0, -1):
                        comm.Send(message, dest=k)
        print ("Prev recvs counter: ", prevRecvCounter)

else:
        message = np.zeros(1)
        Y=np.zeros((size,1))
        sendMult=np.zeros((size,1))
        flag=1
        sleepFlag=1
        iterIndex = 1
        req = comm.Irecv(message, source=pm)
        while iterIndex <= numberOfIter+1:
            if iterIndex == numberOfIter +1:
                comm.Send(Y, dest=pm, tag=iterIndex)
                iterIndex += 1
            else:
                if sleepFlag !=1 :
                    if random.uniform(0, 1) < p:
                        time.sleep(tsleep)
                    sleepFlag = 1
                if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag:  # resets if message recv
                    flag = 1
                    iterIndex += 1
                    counter = 0
                    sendMult = np.zeros((size, 1))
                    sleepFlag=0
                    if iterIndex <= numberOfIter:
                        req = comm.Irecv(message, source=pm)
                if (counter<numberOfComp) and sleepFlag:
                    tet = np.random.rand(size, 1)
                    Y = np.matmul(X, tet)
                    sendMult += Y
                    counter +=1
                if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag:  # resets if message recv
                        iterIndex += 1
                        flag = 1
                        counter = 0
                        sendMult = np.zeros((size, 1))
                        sleepFlag=0
                        if iterIndex <= numberOfIter:
                            req = comm.Irecv(message, source=pm)
                        #print("message changed")
                if counter == numberOfComp and flag and sleepFlag:
                    comm.Send(sendMult, dest=pm, tag=iterIndex)
                    flag = 0
