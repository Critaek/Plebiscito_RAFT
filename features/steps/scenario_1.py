from behave import *
import threading
from src.raft_node import node
import numpy as np
import datetime
import sys
from src.dataset import JobList
from src.topology import topo
import time
from simpleRaft.leader import Leader

def message_data(job_id, user, num_gpu, num_cpu, duration, job_name, submit_time, gpu_type, num_inst, size, bandwidth, layer_number, num_edges, min_layer_number):
    
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


@given('5 nodes, 4 Follower and a Leader')
def step_impl(context):
    context.num_edges = 5
    nodes_thread = []
    dataset='./df_dataset.csv'
    counter = 0 #Messages counter
    context.req_number = 2
    a = 1

    #Data analisys
    context.job_list_instance = JobList(dataset, num_jobs_limit=context.req_number)
    context.job_list_instance.select_jobs()
    job_dict = {job['job_id']: job for job in context.job_list_instance.job_list} # to find jobs by id

    #NN model
    context.layer_number = 6 
    context.min_layer_number = 2 #Min number of layers per node
    max_layer_number = context.layer_number/2 #Max number of layers per node

    tot_gpu = 0 
    tot_cpu = 0 
    tot_bw = 0 
    for d in context.job_list_instance.job_list:
        tot_gpu += d["num_gpu"] 
        tot_cpu += d["num_cpu"] 
        tot_bw += float(d['read_count'])

    print('cpu: ' +str(tot_cpu))
    print('gpu: ' +str(tot_gpu))
    print('bw: ' +str(tot_bw))
    cpu_gpu_ratio = tot_cpu / tot_gpu
    print('cpu_gpu_ratio: ' +str(cpu_gpu_ratio))

    node_gpu=float(tot_gpu/context.num_edges)
    node_cpu=float(tot_cpu/context.num_edges) 
    node_bw=float(tot_bw/(context.num_edges*context.layer_number/context.min_layer_number))

    node_gpu = 1000000000
    # node_cpu = 1000000000
    # node_bw = 1000000000

    num_clients=len(set(d["user"] for d in context.job_list_instance.job_list))

    #Build Topolgy
    t = topo(func_name='complete_graph', max_bandwidth=node_bw, min_bandwidth=node_bw/2,num_clients=num_clients, num_edges=context.num_edges)

    leader = node(0, node_gpu, node_cpu, t.b)
    leader.server._state = Leader()
    leader.leader = True
    context.nodes = []

    job_ids = []
    
    # context.nodes[0] will be the leader
    context.nodes.append(leader)
    for i in range(1, context.num_edges):
        context.nodes.append(node(i, node_gpu, node_cpu, t.b))

    for s in context.nodes:
        s.set_neighbors(context.nodes)
        s.there_is_a_leader = True 

    # Send the jobs
    for job in context.job_list_instance.job_list:
            job_ids.append(job['job_id'])
            for j in range(context.num_edges):
                context.nodes[j].append_data(
                    message_data
                    (
                        job['job_id'],
                        job['user'],
                        job['num_gpu'],
                        job['num_cpu'],
                        job['duration'],
                        job['job_name'],
                        job['submit_time'],
                        job['gpu_type'],
                        job['num_inst'],
                        job['size'],
                        job['read_count'],
                        context.layer_number,
                        context.num_edges,
                        context.min_layer_number
                    )
                    )
            
    print(job_ids)

    for i in range(context.num_edges):
            nodes_thread.append(threading.Thread(target=context.nodes[i].work, daemon=True).start())

    for i in range(len(context.nodes)):
        context.nodes[i].join_queue()

    # context.nodes[1] is a follower (averyone rather than 0), check if the log replication is ok
    # We could've used any node, just use a random one
    assert len(context.nodes[1].server._log) == context.req_number

@when('the Leader goes offline')
def step_impl(context):
    context.nodes[0].kill()
    time.sleep(1)
    del context.nodes[0]

@then('the Followers should elect a new Leader')
def step_impl(context):
    flag = False

    for n in context.nodes:  
        if n.leader == True:
             flag = True

    assert flag is True