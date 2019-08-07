import numpy as np
from mpi4py import MPI
import time
import random
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 2500
numberOfworkers=10
numberOfComp=3
numberOfIter=100
threshold = 3
pm=0
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
groupSize = 5
numberOfGroups = numberOfworkers/groupSize
iterIndex=1
compVector = [2,3]
lastTagCounter=0
p=0.2
tsleep=0.005

def messageCheck(req,recvCounter,counter,sendMult):
    if MPI.Request.Test(req):
        req = comm.Irecv(message, source=pm)
        recvCounter += 1
        counter = 0
        sendMult = np.zeros((size, 1))
        print("message changed")
if rank == pm:
        ts = time.time()
        groupOneRecvs = 0
        groupTwoRecvs = 0
        recvVector=np.zeros(int(numberOfGroups))
        message=np.zeros(1)
        while iterIndex <= numberOfIter+1:
            Y = np.empty([size, 1])
            if iterIndex == numberOfIter:
                tf = time.time()
                print(tf - ts)
                while(lastTagCounter < numberOfworkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag()==iterIndex:
                       lastTagCounter += 1
                iterIndex += 1
            else:
                while threshold not in recvVector:
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag() == iterIndex:
                        groupNo = (status.Get_source() -1) / groupSize
                        if recvVector[groupNo] < threshold:
                            recvVector[groupNo] +=1
                recvVector=np.zeros(int(numberOfGroups))
                iterIndex += 1
                message[0] = iterIndex
                if iterIndex % 2 == 0:
                    for k in range(1, numberOfworkers + 1):
                        comm.Send(message, dest=k)
                else:
                    for k in range(numberOfworkers, 0, -1):
                        comm.Send(message, dest=k)

else:
    message = np.zeros(1)
    iterIndex = 1
    groupTag = rank % 2
    flag = 1
    sleepFlag = 1
    sendMult = np.zeros((size, 1))
    req = comm.Irecv(message, source=pm)
    while iterIndex <= numberOfIter+1:
        if iterIndex == numberOfIter + 1:
            comm.Send(sendMult, dest=pm, tag=iterIndex)
            iterIndex += 1
        else:
            if sleepFlag != 1:
                if random.uniform(0, 1) < p:
                    time.sleep(tsleep)
                sleepFlag = 1
            if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag:  # check right away and after message sent.
                iterIndex += 1
                counter = 0
                flag = 1
                sleepFlag = 0
                sendMult = np.zeros((size, 1))
                if iterIndex <= numberOfIter:
                    req = comm.Irecv(message, source=pm)
            if counter < numberOfComp and flag and sleepFlag: # comp block
                tet = np.random.rand(size, 1)
                Y = np.matmul(X, tet)
                sendMult += Y
                counter += 1
            if MPI.Request.Test(req) and iterIndex <=numberOfIter and sleepFlag:  # check after comp
                iterIndex += 1
                counter = 0
                flag=1
                sleepFlag = 0
                sendMult = np.zeros((size, 1))
                if iterIndex <= numberOfIter:
                    req = comm.Irecv(message, source=pm)
            if flag and counter != 0 and sleepFlag: # check the transmission conditions.
                for i in range (len(compVector)): # transmission block
                    if compVector[i]==counter:
                        comm.Send(sendMult, dest=pm, tag=iterIndex)
                        if i == len(compVector) - 1:
                            flag = 0
                        break