'''
This module impelments the behavior of a node
'''

import random
import queue
from datetime import datetime, timedelta
import copy
import logging
import math
from simpleRaft.follower import Follower
from simpleRaft.candidate import Candidate
from simpleRaft.memory_board import MemoryBoard
from simpleRaft.leader import Leader
from simpleRaft.server import Server
from simpleRaft.append_entries import AppendEntriesMessage
import time
import threading

TRACE = 5
DEBUG = logging.DEBUG


class node:

    def __init__(self, id, initial_gpu, initial_cpu, initial_bw):
        self.id = id    # unique edge node id
        self.initial_gpu = initial_gpu
        self.updated_gpu = self.initial_gpu
        self.initial_cpu = initial_cpu
        self.updated_cpu = self.initial_cpu
        self.initial_bw = initial_bw
        self.updated_bw = self.initial_bw
        
        self.q = queue.Queue()
        self.user_requests = []
        self.item={}
        self.bids= {}

        # RAFT implementation from https://github.com/streed/simpleRaft
        self.board = MemoryBoard()
        # When creating a new node, evey node is a Follower
        self.server = Server(self.id, Follower(), [], self.board, [])
        self.server._total_nodes = 1
        self.neighbors = []
        self.neighbors_nodes = []
        self.leader = False
        self.there_is_a_leader = False
        self.there_is_a_candidate = False

        self.exit_event = threading.Event()

    def start_random_timer(self):
        # Values taken from http://thesecretlivesofdata.com/raft/
        timer = random.randint(150, 300)
        started = datetime.now()
        ended = started + timedelta(milliseconds=timer)
        while datetime.now() < ended:
            pass
        
        #After a random timer between 150 and 300 ms, become a candidate
        if self.there_is_a_leader == False and self.there_is_a_candidate == False:
            self.there_is_a_candidate = True
            for n in self.neighbors_nodes:
                n.there_is_a_candidate = True
            self.become_candidate()
        
    def become_candidate(self):
        total_nodes = self.server._total_nodes
        self.server = Server(self.id, Candidate(), [], self.board, self.neighbors)
        # In the SimpleRaft implementation, for some reason, every time we instantiate a new server,
        # we also need to specify the number of nodes like this, making it like self._total_nodes = len(neighbors)
        # would've been better, i kept it like this in order to be as similar as possible to the original implementation
        self.server._total_nodes = total_nodes
        

        for n in self.neighbors:
            n.on_message(n._messageBoard.get_message())
        
        for n in self.neighbors:
            # Also there, if a majority of votes is reached, the server changes state, and some problem arises when counting the voter,
            # (the leader doesn't have to, he is already a leader), adding a function to count the votes (on_vote_received) that just doesn't
            # do anything would've been a good idea
            if type(self.server._state) == Leader:
                self.server._messageBoard.get_message()
            else:
                self.server.on_message(self.server._messageBoard.get_message())
        
        if type(self.server._state) == Leader:
            self.leader = True
            self.state = self.server._state
            self.there_is_a_leader = True
            self.there_is_a_candidate = False

        for n in self.neighbors_nodes:
            n.there_is_a_leader = True
            n.there_is_a_candidate = False

    def check_leader(self):
        flag = False

        for n in self.neighbors_nodes:
            if type(n.server._state) == Leader:
                flag = True
                self.there_is_a_leader = True        

        return flag
    
    def delete_neighbor_node(self, id):
        # Server
        for n in self.neighbors:
            for i in range(len(n._neighbors)):
                if n._neighbors[i]._name == id:
                    del n._neighbors[i]
                    break
            n._total_nodes -= 1

        # Nodes
        for n in self.neighbors_nodes:
            n.there_is_a_leader = False
            for i in range(len(n.neighbors)):
                if n.neighbors_nodes[i].id == id:
                    del n.neighbors_nodes[i]
                    break
    
    def kill(self):
        self.delete_neighbor_node(self.id)
        self.exit_event.set()

    def get_server(self):
        return self.server
    
    def set_neighbors(self, neighbors):
        for n in neighbors:
            if n.id != self.id:
                self.neighbors_nodes.append(n)
                self.neighbors.append(n.get_server())
                self.server._total_nodes += 1
        self.server._neighbors = self.neighbors

    def calculate_bid(self):
        NN_len = len(self.item['NN_gpu']) #6 in our example
        tot_gpu = self.item["num_gpu"]
        tot_cpu = self.item["num_cpu"]
        node_number = self.server._total_nodes
        tot_allocated_layers = 0

        flag = False

        # Not really a bid, the name is that to keep the same notation as the other implementation
        bid = {}
        bid['bid_cpu'] = []
        bid['bid_gpu'] = []
        bid['bid_bw'] = []

        for i in range(int(NN_len/node_number)):
            if len(bid['bid_cpu']) < NN_len:
                pass
            layer_cpu = self.item["NN_cpu"][i]
            layer_gpu = self.item["NN_gpu"][i]
            layer_data_size = self.item["NN_data_size"][i]

            if self.updated_cpu - layer_cpu >= 0 and self.updated_gpu - layer_gpu >= 0 and self.updated_bw - layer_data_size + 0.1 >= 0:
                bid["bid_cpu"].append(self.id)
                self.updated_cpu -= layer_cpu
                tot_cpu -= layer_cpu
                bid["bid_gpu"].append(self.id)
                self.updated_gpu -= layer_gpu
                tot_gpu -= layer_gpu
                bid["bid_bw"].append(self.id)
                tot_allocated_layers += 1
                flag = True
                    
        if flag:
            self.updated_bw -= layer_data_size

        flag = False

        # If there are some other resources to allocate, allocate them in the neighbors
        if tot_cpu >= 0 and tot_gpu >= 0:
            for n in self.neighbors_nodes:
                for i in range(int(NN_len/node_number)):
                    if len(bid['bid_cpu']) < NN_len:
                        pass
                    layer_cpu = self.item["NN_cpu"][i]
                    layer_gpu = self.item["NN_gpu"][i]
                    layer_data_size = self.item["NN_data_size"][i]

                    if n.updated_cpu - layer_cpu >= 0 and n.updated_gpu - layer_gpu >= 0 and n.updated_bw - layer_data_size + 0.1 >= 0:
                        bid["bid_cpu"].append(n.id)
                        n.updated_cpu -= layer_cpu
                        tot_cpu -= layer_cpu
                        bid["bid_gpu"].append(n.id)
                        n.updated_gpu -= layer_gpu
                        tot_gpu -= layer_gpu
                        bid["bid_bw"].append(n.id)
                        tot_allocated_layers += 1
                        flag = True

                if flag:
                    n.updated_bw -= layer_data_size

        return bid

    def append_data(self, d):
        self.q.put(d)

    def join_queue(self):
        self.q.join()

    def work(self):
        # Testing..
        print_flag = True
        while True and not self.exit_event.is_set():
            # First thing to do is to establish a leader
            if not self.leader and self.check_leader() == False:
                self.start_random_timer()

            # This part can be ignored, just for testing purposes
            if self.leader and print_flag:
                print_flag = False
                logging.log(DEBUG, f"Node {self.id} is the leader of this session")

            #Once we have a leader, we can start sending messages
            
            # If we are not the leader
            if not self.leader:
                if self.q.empty() == False:
                    self.q.get()
                    self.q.task_done()
                
                # Reply to eventual messages
                msg = self.server._messageBoard.get_message()
                if msg != None:
                    self.server.on_message(msg)
                
            
            # If we are the leader
            if self.leader:
                # If there are some jobs waiting
                if(self.q.empty() == False):
                    self.item = None
                    self.item = copy.deepcopy(self.q.get())

                    #print(f"self.item = {self.item}")

                    # The leader decides how to split the computation, then sends the message to all the followers,
                    # at the end, every node (leader and followers) will have the same log

                    bid = self.calculate_bid()

                    job_id = self.item["job_id"]
                    # At this point, the rescource allocation is done, we need to communicate it to the other nodes

                    msg = AppendEntriesMessage( self.id, None, self.server._currentTerm, { 
                                "prevLogIndex": self.server._lastLogIndex, 
                                "prevLogTerm": self.server._currentTerm, 
                                "leaderCommit": self.server._commitIndex,  
                                "entries": [ { "term": self.server._currentTerm, "job_id": self.item["job_id"], "value": bid } ] } )
                    
                    self.server.send_message(msg)

                    #for n in self.server._neighbors:
                    #    n.on_message(n._messageBoard.get_message())

                    # Wait for the other nodes to reply
                    time.sleep(0.2)

                    for i in range(len(self.server._messageBoard._board)):
                        self.server._messageBoard.get_message()

                    # Just a test
                    for n in self.server._neighbors:
                        logging.log(DEBUG, f"Follower log from node: {n._name} after handling job {job_id}: {n._log}\n")

                    print(f"Example log from one of the follower: {self.server._neighbors[0]._log}\n")
        
                    self.q.task_done()
                
                else:
                    # If i'm the leader, and no job are in the queue, send hearthbeat messages to all the followers
                    timer = random.randint(150, 300)
                    started = datetime.now()
                    ended = started + timedelta(milliseconds=timer)
                    while datetime.now() < ended:
                        pass
                    
                    self.server._state._send_heart_beat()

                    # Wait for the other nodes to reply
                    time.sleep(0.2)

                    for msg in self.server._messageBoard._board:
                        self.server.on_message( msg )

                    logging.log(DEBUG, f"Heartbeat response ids: {self.server._state._nextIndexes}")


                # print(str(self.q.qsize()) +" polpetta - user:"+ str(self.id) + " job_id: "  + str(self.item['job_id'])  + " from " + str(self.item['user']))

      
      