import numpy as np
from mpi4py import MPI
import time
comm = MPI.COMM_WORLD
rank = comm.Get_rank()
status = MPI.Status()
size = 2500
numberOfWorkers = 10
numberOfIter =100
numberOfpackets = 7
numberOfComp = 3
pm = 0
p = 0.5
X = np.random.rand(size, size) * np.sqrt(1)
counter = 0


if rank == pm:
        ts = time.time()
        message = np.zeros(1)
        for i in range (1,numberOfIter+1):
            message[0] = i
            Y = np.empty([size, 1])
            while counter < numberOfpackets:
                comm.Recv(Y, source=MPI.ANY_SOURCE, tag=message[0], status=status)
                counter += 1
                #print("pm Counter so far",counter)
            #print("!! all packets for the respective tag recieved !!")
            for k in range (1,numberOfWorkers+1):
                req =comm.Send(message, dest=k)
            #print("message Send/bcasted")
            counter = 0
        tf = time.time()
        print(tf-ts)

else:
        message = np.zeros(1)
        Y=np.zeros((size,1))
        sendMult=np.zeros((size,1))
        flag=1
        recvCounter = 0
        req = comm.Irecv(message, source=pm)
        while recvCounter<numberOfIter:
            if (counter<numberOfComp):
                if (recvCounter == numberOfIter):
                    break
                tet = np.random.rand(size, 1)
                Y = np.matmul(X, tet)
                sendMult += Y
                counter +=1
            if MPI.Request.Test(req):  # resets if message recv
                    req = comm.Irecv(message, source=pm)
                    recvCounter += 1
                    flag = 1
                    counter =0
                    sendMult =np.zeros((size,1))
                    #print("message changed")
            if counter == numberOfComp and flag:
                sendTag = recvCounter + 1
                comm.Send(sendMult, dest=pm, tag=sendTag)
                flag = 0
            if MPI.Request.Test(req):  # resets if message recv
                req = comm.Irecv(message, source=pm)
                flag = 1
                recvCounter += 1
                counter=0
                sendMult = np.zeros((size, 1))

            #print("sent, message is ", message[0], "rank is", rank, "recvCounter, ", recvCounter)
        #print("recvC, ", recvCounter)