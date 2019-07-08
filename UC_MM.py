import numpy as np
from mpi4py import MPI
import time
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 3000
numberOfworkers=10
numberOfComp=3
numberOfIter=1000
pm=0
w=1
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
prevRecvCounter=0
lastTagCounter = 0
tsleep = 0.018
p=0.4

if rank == pm:
        ts = time.time()
        placements=0;
        message=np.zeros(1)
        tagVector = np.zeros(numberOfworkers)
        while counter <numberOfIter+1:
            counter += 1
            Y = np.empty([size, 1])
            if counter == numberOfIter+1:
                tf = time.time()
                print(tf - ts)
                while(lastTagCounter<numberOfworkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag() == counter:
                        lastTagCounter +=1
                    else:
                        prevRecvCounter +=1
            else:
                while (sum(tagVector) < numberOfworkers):
                        comm.Recv(Y, source=MPI.ANY_SOURCE, tag = MPI.ANY_TAG, status = status)
                        if status.Get_tag() == counter:
                            tagVector[int(Y[0][0])-1] = 1
                        else:
                            prevRecvCounter += 1
                tagVector=np.zeros(numberOfworkers)
                message[0] = counter+1
                if counter % 2 == 0:
                    for k in range(1, numberOfworkers + 1):
                        comm.Send(message, dest=k)
                else:
                    for k in range(numberOfworkers, 0, -1):
                        comm.Send(message, dest=k)
        print("PrevRecvsCounter is ",prevRecvCounter)
else:
        message = np.zeros(1)
        req = comm.Irecv(message, source=pm)
        i = rank
        sleepFlag=1
        iterIndex = 1
        while iterIndex <= numberOfIter+1:
            if iterIndex == numberOfIter+1:
               comm.Send(Y, dest=pm, tag=iterIndex)
               iterIndex +=1
            else:
                if sleepFlag !=1 :
                    if random.uniform(0, 1) < p:
                        time.sleep(tsleep)
                    sleepFlag = 1
                if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag: # Check the message after send
                   i = rank  # reset the i when message came
                   iterIndex += 1
                   sleepFlag=0
                   if iterIndex <= numberOfIter:
                       req = comm.Irecv(message, source=pm)
                if (i < rank+numberOfComp) and iterIndex <= numberOfIter and sleepFlag:
                    tet = np.random.rand(size,1)
                    Y = np.matmul(X,tet)
                if MPI.Request.Test(req) and iterIndex <= numberOfIter and sleepFlag: # Check the message
                    i = rank  # reset the i when message came
                    iterIndex += 1
                    sleepFlag = 0
                    if iterIndex <= numberOfIter:
                        req = comm.Irecv(message, source=pm)
                if (i < rank+numberOfComp) and iterIndex <= numberOfIter and sleepFlag: # Transmission block
                    if (i <= numberOfworkers):# tag = rank
                        Y[0][0] = i
                        comm.Send(Y, dest = pm, tag = iterIndex)
                        i += 1
                    else: # Tags goes back to start if selected tag is bigger than tag preselected tag size
                        Y[0][0] = i % numberOfworkers
                        comm.Send(Y, dest=pm, tag= iterIndex)
                        i +=1

