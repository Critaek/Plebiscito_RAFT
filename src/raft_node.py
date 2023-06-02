'''
This module impelments the behavior of a node
'''

import random
import queue
import src.config as config
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

TRACE = 5
DEBUG = logging.DEBUG


class node:

    def __init__(self, id):
        self.id = id    # unique edge node id
        self.initial_gpu = float(config.node_gpu)
        self.updated_gpu = self.initial_gpu
        self.initial_cpu = float(config.node_cpu)
        self.updated_cpu = self.initial_cpu
        self.initial_bw = config.t.b
        self.updated_bw = self.initial_bw
        
        self.q = queue.Queue()
        self.user_requests = []
        self.item={}
        self.bids= {}

        # RAFT implementation from https://github.com/streed/simpleRaft
        self.board = MemoryBoard()
        # When creating a new node, evey node is a Follower
        self.state = Follower()
        self.server = Server(self.id, self.state, [], self.board, [])
        self.server._total_nodes = 1
        self.neighbors = []
        self.neighbors_nodes = []
        self.leader = False
        self.there_is_a_leader = False

    def start_random_timer(self):
        # Values taken from http://thesecretlivesofdata.com/raft/
        timer = random.randint(150, 300)
        started = datetime.now()
        ended = started + timedelta(milliseconds=timer)
        while datetime.now() < ended:
            pass
        
        #After a random timer between 150 and 300 ms, become a candidate
        if self.there_is_a_leader == False:
            self.become_candidate()
        
    def become_candidate(self):
        self.state = Candidate()
        total_nodes = self.server._total_nodes
        self.server = Server(self.id, self.state, [], self.board, self.neighbors)

        for n in self.neighbors:
            n.on_message(n._messageBoard.get_message())
        
        for n in self.neighbors:
            self.server.on_message(self.server._messageBoard.get_message())
        
        if type(self.server._state) == Leader:
            self.leader = True
            self.state = self.server._state
            self.server._total_nodes = total_nodes
            self.there_is_a_leader = True

        for n in self.neighbors_nodes:
            n.there_is_a_leader = True

    def get_server(self):
        return self.server
    
    def set_neighbors(self, neighbors):
        for n in neighbors:
            if n.id != self.id:
                self.neighbors_nodes.append(n)
                self.neighbors.append(n.get_server())
                self.server._total_nodes += 1
        self.server._neighbors = self.neighbors

    def append_data(self, d):
        self.q.put(d)

    def join_queue(self):
        self.q.join()

    def work(self):
        # Testing..
        print_flag = True
        while True:
            # First thing to do is to establish a leader
            if self.there_is_a_leader == False:
                self.start_random_timer()

            # This part can be ignored, just for testing purposes
            if self.leader and print_flag:
                print_flag = False
                print(f"Sono leader!! id: {self.id}")

            #Once we have a leader, we can start sending messages
            
            # If we are not the leader
            if self.q.empty() == False and not self.leader:
                self.q.get()
                self.q.task_done()

            if(self.q.empty() == False) and self.leader:

                config.counter += 1 # Increment message counter
                self.item=None
                self.item = copy.deepcopy(self.q.get())

                #print(f"self.item = {self.item}")

                # The leader decides how to split the computation, then sends the message to all the followers,
                # at the end, every node (leader and followers) will have the same log

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
                    layer_cpu = self.item["NN_cpu"][i]
                    layer_gpu = self.item["NN_gpu"][i]
                    layer_data_size = self.item["NN_data_size"][i]

                    if self.updated_cpu - layer_cpu >= 0 and self.updated_gpu - layer_gpu >= 0 and self.updated_bw - layer_data_size >= 0:
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
                            layer_cpu = self.item["NN_cpu"][i]
                            layer_gpu = self.item["NN_gpu"][i]
                            layer_data_size = self.item["NN_data_size"][i]

                            if n.updated_cpu - layer_cpu >= 0 and n.updated_gpu - layer_gpu >= 0 and n.updated_bw - layer_data_size >= 0:
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

                #print(f"bid: {bid}")

                job_id = self.item["job_id"]
                # At this point, the rescource allocation is done, we need to communicate it to the other nodes

                msg = AppendEntriesMessage( self.id, None, self.server._currentTerm, { 
                            "prevLogIndex": self.server._lastLogIndex, 
							"prevLogTerm": self.server._currentTerm, 
							"leaderCommit": self.server._commitIndex,  
							"entries": [ { "term": 1, "job_id": self.item["job_id"], "value": bid } ] } )
                
                self.server.send_message(msg)

                for n in self.server._neighbors:
                    n.on_message(n._messageBoard.get_message())

                # Just a test
                for n in self.server._neighbors:
                    print(f"Follower log after handling job {job_id}: {n._log}")
      
                self.q.task_done()

                # print(str(self.q.qsize()) +" polpetta - user:"+ str(self.id) + " job_id: "  + str(self.item['job_id'])  + " from " + str(self.item['user']))

      
      