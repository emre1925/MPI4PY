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
threshold = 3
pm=0
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0
compVector = [2,3]

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
        message=np.zeros(1)
        while counter < numberOfIter:
            Y = np.empty([size, 1])
            counter += 1
            message[0] = counter
            while groupTwoRecvs < threshold or groupOneRecvs < threshold:
                comm.Recv(Y, source=MPI.ANY_SOURCE, tag=counter, status=status)
                if status.Get_source() % 2 == 0 and groupOneRecvs < threshold:
                    groupOneRecvs += 1
                if status.Get_source() % 2 == 1 and groupTwoRecvs < threshold:
                    groupTwoRecvs +=1
            groupOneRecvs = 0
            groupTwoRecvs = 0
            for i in range(1,numberOfworkers + 1):
                comm.Send(message, dest=i)
        tf = time.time()
        print(tf-ts)

else:
    message = np.zeros(1)
    recvCounter = 0
    groupTag= rank % 2
    flag = 1
    sendMult = np.zeros((size, 1))
    req = comm.Irecv(message, source=pm)
    while recvCounter<numberOfIter:
        if MPI.Request.Test(req):  # check right away and after message sent.
            req = comm.Irecv(message, source=pm)
            recvCounter += 1
            counter = 0
            flag = 1
            sendMult = np.zeros((size, 1))
            print("message changed while waiting")
        if counter < numberOfComp and flag: # comp block
            tet = np.random.rand(size, 1)
            Y = np.matmul(X, tet)
            sendMult += Y
            counter += 1
        if MPI.Request.Test(req):  # check after comp
            req = comm.Irecv(message, source=pm)
            recvCounter += 1
            counter = 0
            flag=1
            sendMult = np.zeros((size, 1))
            print("message changed in comp")
        if flag and counter != 0: # check the transmission conditions.
            for i in range (len(compVector)): # transmission block
                if compVector[i]==counter:
                    comm.Send(sendMult, dest=pm, tag=recvCounter+1)
                    if i == len(compVector) - 1:
                        flag = 0
                    print("sent 1 recv counter recv and message is ",recvCounter,message[0])
                    break