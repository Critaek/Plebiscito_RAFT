'''
Configuration variables module
'''

from src.raft_node import node
import numpy as np
import datetime
import sys
from src.dataset import JobList
from src.topology import topo

counter = 0 #Messages counter
req_number = int(sys.argv[1]) #Total number of requests
a = float(sys.argv[2]) #Multiplicative factor
num_edges = int(sys.argv[3]) #Nodes number 
filename = str(sys.argv[4])

#NN model
layer_number = 6 
min_layer_number = 2 #Min number of layers per node
max_layer_number = layer_number/2 #Max number of layers per node


dataset='./df_dataset.csv'

#Data analisys
job_list_instance = JobList(dataset, num_jobs_limit=req_number)
job_list_instance.select_jobs()
job_dict = {job['job_id']: job for job in job_list_instance.job_list} # to find jobs by id

print('jobs number = ' + str(len(job_dict)))

# print(job_list_instance.job_list[0])

# calculate total bw, cpu, and gpu needed
tot_gpu = 0 
tot_cpu = 0 
tot_bw = 0 
for d in job_list_instance.job_list:
    tot_gpu += d["num_gpu"] 
    tot_cpu += d["num_cpu"] 
    tot_bw += float(d['read_count'])

print('cpu: ' +str(tot_cpu))
print('gpu: ' +str(tot_gpu))
print('bw: ' +str(tot_bw))
cpu_gpu_ratio = tot_cpu / tot_gpu
print('cpu_gpu_ratio: ' +str(cpu_gpu_ratio))

node_gpu=float(tot_gpu/num_edges)
node_cpu=float(tot_cpu/num_edges) 
node_bw=float(tot_bw/(num_edges*layer_number/min_layer_number))

node_gpu = 1000000000
# node_cpu = 1000000000
# node_bw = 1000000000

num_clients=len(set(d["user"] for d in job_list_instance.job_list))


#Build Topolgy
t = topo(func_name='complete_graph', max_bandwidth=node_bw, min_bandwidth=node_bw/2,num_clients=num_clients, num_edges=num_edges)

#Create nodes
server_list = []
for row in range(num_edges):
    server_list.append(node(row))

for s in server_list:
    s.set_neighbors(server_list)

# At this point, each node knows every neighbor and how many nodes are there in total
nodes = server_list

def message_data(job_id, user, num_gpu, num_cpu, duration, job_name, submit_time, gpu_type, num_inst, size, bandwidth):
    
    gpu = round(num_gpu / layer_number, 2)
    cpu = round(num_cpu / layer_number, 2)
    bw = round(float(bandwidth) / (num_edges*layer_number/min_layer_number), 2)

    NN_gpu = np.ones(layer_number) * gpu
    NN_cpu = np.ones(layer_number) * cpu
    NN_data_size = np.ones(layer_number) * bw
    
    data = {
        "job_id": int(),
        "user": int(),
        "num_gpu": int(),
        "num_cpu": int(),
        "duration": int(),
        "job_name": int(),
        "submit_time": int(),
        "gpu_type": int(),
        "num_inst": int(),
        "size": int(),
        "edge_id":int(),
        "NN_gpu": NN_gpu,
        "NN_cpu": NN_cpu,
        "NN_data_size": NN_data_size
        }


    data['edge_id']=None
    data['job_id']=job_id
    data['user']=user
    data['num_gpu']=num_gpu
    data['num_cpu']=num_cpu
    data['duration']=duration
    data['job_name']=job_name
    data['submit_time']=submit_time
    data['gpu_type']=gpu_type
    data['num_inst']=num_inst
    data['size']=size
    data['job_id']=job_id

    return data



"""
NOTES

I -> Index set of agents {1,..., Na}; Na -> Max Agent Number
J -> Index set of tasks {1,..., Nt}; Nt -> tasks set
Lt -> max lenght of the bundle

b_i -> bundle for task i
tao_i -> vector of task execution time for agent i
s_i -> communication timestamps with other agents carried by agent i
y_i -> list of winning bids for all tasks carried by agent i
z_i -> list of winning agents for all tasks carried by agent i


Nothe that each task L_t
"""