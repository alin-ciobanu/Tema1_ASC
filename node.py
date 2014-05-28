"""
    This module represents a cluster's computational node.

    Computer Systems Architecture Course
    Assignment 1 - Cluster Activity Simulation
    March 2014
"""

from threading import Thread, Condition
import Queue
import sys
from time import sleep

class Node:
    """
        Class that represents a cluster node with computation and storage
        functionalities.
    """
    
    queue = Queue.Queue()
    condition = Condition()
    condition2 = Condition()

    def __init__(self, node_id, matrix_size):
        """
            Constructor.

            @type node_id: Integer
            @param node_id: an integer less than 'matrix_size' uniquely
                identifying the node
            @type matrix_size: Integer
            @param matrix_size: the size of the matrix A
        """
        self.node_id = node_id
        self.matrix_size = matrix_size
        self.datastore = None
        self.nodes = None
        self.total_threads = []
        self.condition_final = Condition()

    def __str__(self):
        """
            Pretty prints this node.

            @rtype: String
            @return: a string containing this node's id
        """
        return "Node %d" % self.node_id

    def set_datastore(self, datastore):
        """
            Gives the node a reference to its datastore. Guaranteed to be called
            before the first call to 'get_x'.
enaste .
            @type datastore: Datastore
            @param datastore: the datastore associated with this node
        """
        self.datastore = datastore


    def set_nodes(self, nodes):
        """
            Informs the current node of the other nodes in the cluster. 
            Guaranteed to be called before the first call to 'get_x'.

            @type nodes: List of Node
            @param nodes: a list containing all the nodes in the cluster
        """
        self.nodes = nodes

    def get_x(self):
        """
            Computes the x value corresponding to this node. This method is
            invoked by the tester. This method must block until the result is
            available.

            @rtype: (Float, Integer)
            @return: the x value and the index of this variable in the solution
                vector
        """

        if self.node_id == self.matrix_size - 1: # master node
            thread = MasterThread(Node.queue, self.datastore, self, self.matrix_size, self.nodes, Node.condition, Node.condition2)
            self.datastore.register_thread(self, thread)
            thread.start()
            self.total_threads.append(thread)

        Node.condition2.acquire()
        while True:
            if Node.queue.qsize() > 0:
                break
            Node.condition2.wait()
        Node.condition2.release()

        q = Queue.Queue()
        condition3 = Condition()
        self.get_final_result(q, condition3)
        condition3.acquire()
        while True:
            if q.qsize() == 1:
                break
            condition3.wait()
        condition3.release()
        elem = q.get()

        return (elem[0], elem[1])


    def get_final_result(self, queue, condition3):
        thread = ThreadFinalRes(self.datastore, self, queue, condition3)
        self.datastore.register_thread(self, thread)
        thread.start()
        self.total_threads.append(thread)

    def get_elem(self, col, queue):
        thread = ThreadPuttingItsElemInQueue(col, queue, self.datastore, self, Node.condition)
        self.datastore.register_thread(self, thread)
        thread.start()
        self.total_threads.append(thread)

    def put_elem(self, col, elem):
        thread = ThreadWritingElem(col, elem, self.datastore, self, Node.condition, Node.queue)
        self.datastore.register_thread(self, thread)
        thread.start()
        self.total_threads.append(thread)

    def get_b_elem(self, queue):
        thread = ThreadGettingB(self.datastore, self, queue, Node.condition)
        self.datastore.register_thread(self, thread)
        thread.start()
        self.total_threads.append(thread)

    def put_b_elem(self, b):
        thread = ThreadWritingB(self.datastore, self, b, Node.condition, Node.queue)
        self.datastore.register_thread(self, thread)
        thread.start()
        self.total_threads.append(thread)

    def shutdown(self):
        """
            Instructs the node to shutdown (terminate all threads). This method
            is invoked by the tester. This method must block until all the
            threads started by this node terminate.
        """
        
        Node.queue = Queue.Queue()
        Node.condition = Condition()
        Node.condition2 = Condition()
        for i in range(len(self.total_threads)):
            self.total_threads[i].join()


class MasterThread(Thread):

    def __init__(self, queue, datastore, node, matrix_size, nodes, condition, condition2):
        Thread.__init__(self)
        self.queue = queue
        self.datastore = datastore
        self.node = node
        self.matrix_size = matrix_size
        self.nodes = nodes
        self.condition = condition
        self.condition2 = condition2

    def run(self):
    
        for k in range(0, self.matrix_size - 1):
            for i in range(k + 1, self.matrix_size):
                self.nodes[i].get_elem(k, self.queue)
                self.nodes[k].get_elem(k, self.queue)
                self.condition.acquire()
                while True:
                    if self.queue.qsize() == 2:
                        break
                    self.condition.wait()
                self.condition.release()

                Aik = -1
                Akk = -1
                while self.queue.empty() == False:
                    elem = self.queue.get()
                    if elem[1] == i:
                        Aik = elem[0]
                    else:
                        Akk = elem[0]
                m = Aik / Akk

                Aij = -1
                Akj = -1
                for j in range (k, self.matrix_size):
                    self.nodes[i].get_elem(j, self.queue)
                    self.nodes[k].get_elem(j, self.queue)
                    self.condition.acquire()
                    while True:
                        if self.queue.qsize() == 2:
                            break
                        self.condition.wait()
                    self.condition.release()

                    while self.queue.empty() == False:
                        elem = self.queue.get()
                        if elem[1] == i:
                            Aij = elem[0]
                        else:
                            Akj = elem[0]

                    Aij = Aij - m * Akj
                    self.nodes[i].put_elem(j, Aij)
                    self.condition.acquire()
                    while True:
                        if self.queue.qsize() == 1:
                            break
                        self.condition.wait()
                    self.condition.release()
                    self.queue.get()

                bk = -1
                bi = -1
                self.nodes[i].get_b_elem(self.queue)
                self.nodes[k].get_b_elem(self.queue)
                self.condition.acquire()
                while True:
                    if self.queue.qsize() == 2:
                        break
                    self.condition.wait()
                self.condition.release()
                
                while self.queue.empty() == False:
                    elem = self.queue.get()
                    if elem[1] == i:
                        bi = elem[0]
                    else:
                        bk = elem[0]

                bi = bi - m * bk
                self.nodes[i].put_b_elem(bi)
                self.condition.acquire()
                while True:
                        if self.queue.qsize() == 1:
                            break
                        self.condition.wait()
                self.condition.release()
                self.queue.get()

        bn1 = -1
        An1n1 = -1
        self.nodes[self.matrix_size - 1].get_elem(self.matrix_size - 1, self.queue)
        self.nodes[self.matrix_size - 1].get_b_elem(self.queue)
        self.condition.acquire()
        while True:
            if self.queue.qsize() == 2:
                break
            self.condition.wait()
        self.condition.release()
        while self.queue.empty() == False:
            elem = self.queue.get()
            if elem[2] == -1:
                bn1 = elem[0]
            else:
                An1n1 = elem[0]
        bn1 = bn1 / An1n1

        self.nodes[self.matrix_size - 1].put_b_elem(bn1)
        self.condition.acquire()
        while True:
            if self.queue.qsize() == 1:
                break
            self.condition.wait()
        self.condition.release()
        self.queue.get()

        for i in range(self.matrix_size - 2, -1, -1):
            bi = -1
            self.nodes[i].get_b_elem(self.queue)
            self.condition.acquire()
            while True:
                if self.queue.qsize() == 1:
                    break
                self.condition.wait()
            self.condition.release()
            elem = self.queue.get()
            bi = elem[0]
            sum = bi

            for j in range (i + 1, self.matrix_size):
                Aij = -1
                bj = -1
                self.nodes[i].get_elem(j, self.queue)
                self.nodes[j].get_b_elem(self.queue)
                self.condition.acquire()
                while True:
                    if self.queue.qsize() == 2:
                        break
                    self.condition.wait()
                self.condition.release()
                while self.queue.empty() == False:
                    elem = self.queue.get()
                    if elem[2] == -1:
                        bj = elem[0]
                    else:
                        Aij = elem[0]
                sum -= Aij * bj

            Aii = -1
            self.nodes[i].get_elem(i, self.queue)
            self.condition.acquire()
            while True:
                if self.queue.qsize() == 1:
                    break
                self.condition.wait()
            self.condition.release()
            elem = self.queue.get()
            Aii = elem[0]

            bi = sum / Aii
            self.nodes[i].put_b_elem(bi)
            self.condition.acquire()
            while True:
                if self.queue.qsize() == 1:
                    break
                self.condition.wait()
            self.condition.release()
            self.queue.get()

        self.condition2.acquire()
        for i in range(0, self.matrix_size):
            self.queue.put([-1, -1, -1])
        self.condition2.notifyAll()
        self.condition2.release()


class ThreadPuttingItsElemInQueue(Thread):
    def __init__(self, k, queue, datastore, node, condition):
        Thread.__init__(self)
        self.k = k
        self.queue = queue
        self.datastore = datastore
        self.node = node
        self.condition = condition

    def run(self):
        self.condition.acquire()
        elem = self.datastore.get_A(self.node, self.k)
        self.node_id = self.node.node_id
        data = [elem, self.node_id, self.k]
        self.queue.put(data)
        self.condition.notify()
        self.condition.release()


class ThreadWritingElem(Thread):
    def __init__(self, col, elem, datastore, node, condition, queue):
        Thread.__init__(self)
        self.col = col
        self.datastore = datastore
        self.node = node
        self.elem = elem
        self.queue = queue
        self.condition = condition

    def run(self):
        self.condition.acquire()
        self.datastore.put_A(self.node, self.col, self.elem)
        data = [-1, -1, -1]
        self.queue.put(data)
        self.condition.notify()
        self.condition.release()        

class ThreadGettingB(Thread):
    def __init__(self, datastore, node, queue, condition):
        Thread.__init__(self)
        self.line = node.node_id
        self.datastore = datastore
        self.node = node
        self.queue = queue
        self.condition = condition

    def run(self):
        self.condition.acquire()
        b = self.datastore.get_b(self.node)
        line = self.node.node_id
        data = [b, line, -1] # se trimite -1 pentru a sti ca e vorba de b
        self.queue.put(data)
        self.condition.notify()
        self.condition.release()

class ThreadWritingB(Thread):
    def __init__(self, datastore, node, b, condition, queue):
        Thread.__init__(self)
        self.datastore = datastore
        self.node = node
        self.b = b
        self.condition = condition
        self.queue = queue

    def run(self):
        self.condition.acquire()
        self.datastore.put_b(self.node, self.b)
        data = [-1, -1, -1]
        self.queue.put(data)
        self.condition.notify()
        self.condition.release()

class ThreadFinalRes(Thread):
    def __init__(self, datastore, node, queue, condition3):
        Thread.__init__(self)
        self.datastore = datastore
        self.node = node
        self.queue = queue
        self.condition3 = condition3
        
    def run(self):
        self.condition3.acquire()
        b = self.datastore.get_b(self.node)
        data = [b, self.node.node_id, -1]
        self.queue.put(data)
        self.condition3.notify()
        self.condition3.release()

