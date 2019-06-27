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
pm=0
version=1
w=1
p = 0.5
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0

if rank == pm:
        ts = time.time()
        placements=0;
        message=np.zeros(1)
        tagVector = np.zeros(numberOfworkers)
        while counter <numberOfIter:
            counter += 1
            Y = np.empty([size, 1])
            while (sum(tagVector) < numberOfworkers):
                    comm.Recv(Y, source=MPI.ANY_SOURCE, tag = counter, status = status)
                    tagVector[int(Y[0][0])-1] = 1;
            #print(tagVector)
            tagVector=np.zeros(numberOfworkers)
            message[0]=counter
            for i in range (1,numberOfworkers+1): # sends a confirmed message message for each iter // 1 to 1 2 to 2 etc
                 comm.Send(message, dest = i)
        tf = time.time()
        print(tf-ts)
else:
        message = np.zeros(1)
        req = comm.Irecv(message, source=pm)
        i = rank
        while counter < numberOfIter:
            if (i < rank+numberOfComp) and counter != numberOfIter:
                tet = np.random.rand(size,1)
                Y = np.matmul(X,tet)
            if MPI.Request.Test(req): # Check the message
               req = comm.Irecv(message, source=pm)
               i = rank # reset the i when message came
               counter += 1
               #print("message is ", message[0], "rank is ", rank,"counter is", )
            if (i < rank+numberOfComp) and counter != numberOfIter: # Transmission block
                if (i <= numberOfworkers):# tag = rank
                    Y[0][0] = i
                    comm.Send(Y, dest = pm, tag = counter+1)
                    #print("sent", "rank, i,counter", rank, Y[0][0], counter + 1)
                    i += 1
                else: # Tags goes back to start if selected tag is bigger than tag preselected tag size
                    Y[0][0] = i % numberOfworkers
                    comm.Send(Y, dest=pm, tag= counter+1)
                    #print("sent", "rank, i,counter", rank, Y[0][0], counter + 1)
                    i +=1
            if MPI.Request.Test(req): # Check the message after send
               req = comm.Irecv(message, source=pm)
               i = rank # reset the i when message came
               counter += 1
               #print("message is ", message[0], "rank is ", rank,"counter is", counter)
