import numpy as np
from mpi4py import MPI
import time
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
version = 1
compVector = [2,3]

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
        while counter < numberOfIter:
            Y = np.empty([size, 1])
            counter += 1
            message[0] = counter
            recvVector = np.zeros([numberOfGroups,groupSize])
            while (sum(requiredCompIndex <= np.sum(recvVector, axis=1)).astype(int)) < numberOfGroups:
                comm.Recv(Y, source=MPI.ANY_SOURCE, tag=counter, status = status)
                #print("recv")
                a = groupIndex(status.Get_source(),groupSize,int(Y[0][0]))
                recvVector[a[0]][a[1]-1] = 1
                #print("recvV is", recvVector)
            for i in range(1,numberOfworkers + 1):
                comm.Send(message, dest=i)
            #print("bcasted")
        tf = time.time()
        print(tf-ts)

else:
    message = np.zeros(1)
    recvCounter = 0
    flag = 1
    sendMult = np.zeros([size, 1])
    indexId = 0
    req = comm.Irecv(message, source=pm)
    while recvCounter<numberOfIter:
        if MPI.Request.Test(req):  # check right away and after message sent.
            req = comm.Irecv(message, source=pm)
            recvCounter += 1
            counter = 0
            indexId = 0
            flag = 1
            sendMult = np.zeros((size, 1))
            #print("message changed while waiting")
        if counter < numberOfComp and flag and recvCounter != numberOfIter: # comp block
            tet = np.random.rand(size, 1)
            Y = np.matmul(X, tet)
            sendMult += Y
            counter += 1
        if MPI.Request.Test(req):  # check after comp
            req = comm.Irecv(message, source=pm)
            recvCounter += 1
            counter = 0
            indexId = 0
            flag=1
            sendMult = np.zeros((size, 1))
            #print("message changed in comp")
        if flag and counter != 0 and indexId < len(compVector) : # check the transmission conditions.
            for i in range (len(compVector)): # transmission block
                if compVector[i]==counter:
                        sendMult[0][0] = (indexId)
                        comm.Send(sendMult, dest=pm, tag=recvCounter+1)
                        indexId += 1
                        if i == len(compVector)-1:
                            flag =0
                        #print("1st, infos: sendmult, rank",sendMult[0][0],rank)
                        break