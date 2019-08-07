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
requiredCompIndex = 4
numberOfGroups = 2
groupSize= numberOfworkers/numberOfGroups
pm=0
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
iterIndex =1
version = 1
lastTagCounter=0
compVector = [2,3]
p=0.2
tsleep=0.005

def groupIndex(rank, groupSize, indexId):
    groupTag= (rank-1)/groupSize
    groupRank = ( ((rank-1) %groupSize)) + 1
    groupPlacement = ((groupRank + indexId) % groupSize+1) + ((groupRank+indexId)/(groupSize+1))
    a = [groupTag, groupPlacement]
    return a

if rank == pm:
        ts = time.time()
        groupOneRecvs = 0
        groupTwoRecvs = 0
        message=np.zeros(1)
        while iterIndex <= numberOfIter+1:
            Y = np.empty([size, 1])

            recvVector = np.zeros([numberOfGroups,groupSize])
            if iterIndex == numberOfIter +1:
                tf = time.time()
                print(tf - ts)
                while(lastTagCounter < numberOfworkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
                    if status.Get_tag()==iterIndex:
                       lastTagCounter += 1
                iterIndex += 1
            else:
                while (sum(requiredCompIndex <= np.sum(recvVector, axis=1)).astype(int)) < numberOfGroups:
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status = status)
                    #print("recv")
                    if status.Get_tag() == iterIndex:
                        a = groupIndex(status.Get_source(),groupSize,int(Y[0][0]))
                        recvVector[a[0]][a[1]-1] = 1
                    #print("recvV is", recvVector)
            message[0] = iterIndex
            if iterIndex % 2 == 0:
                for k in range(1, numberOfworkers + 1):
                    comm.Send(message, dest=k)
            else:
                for k in range(numberOfworkers, 0, -1):
                    comm.Send(message, dest=k)
            #print("bcasted")
            iterIndex += 1

else:
    message = np.zeros(1)
    recvCounter = 0
    flag = 1
    sleepFlag=1
    sendMult = np.zeros([size, 1])
    indexId = 0
    req = comm.Irecv(message, source=pm)
    while iterIndex <= numberOfIter +1:
        if iterIndex == numberOfIter + 1:
            comm.Send(sendMult, dest=pm, tag=iterIndex)
            iterIndex += 1
        else:
            if sleepFlag != 1:
                if random.uniform(0, 1) < p:
                    time.sleep(tsleep)
                sleepFlag = 1
            if MPI.Request.Test(req) and sleepFlag:  # check right away and after message sent.
                req = comm.Irecv(message, source=pm)
                iterIndex += 1
                counter = 0
                indexId = 0
                flag = 1
                sleepFlag = 0
                sendMult = np.zeros((size, 1))
                #print("message changed while waiting")
            if counter < numberOfComp and flag and iterIndex <=numberOfIter and sleepFlag: # comp block
                tet = np.random.rand(size, 1)
                Y = np.matmul(X, tet)
                sendMult += Y
                counter += 1
            if MPI.Request.Test(req) and sleepFlag:  # check after comp
                req = comm.Irecv(message, source=pm)
                iterIndex += 1
                counter = 0
                indexId = 0
                flag=1
                sleepFlag = 0
                sendMult = np.zeros((size, 1))
                #print("message changed in comp")
            if flag and counter != 0 and indexId < len(compVector) and sleepFlag: # check the transmission conditions.
                for i in range (len(compVector)): # transmission block
                    if compVector[i]==counter:
                            sendMult[0][0] = (indexId)
                            comm.Send(sendMult, dest=pm, tag=recvCounter+1)
                            indexId += 1
                            if i == len(compVector)-1:
                                flag =0
                            #print("1st, infos: sendmult, rank",sendMult[0][0],rank)
                            break