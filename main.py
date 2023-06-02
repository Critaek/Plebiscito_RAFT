import threading
import datetime
import src.config as c
import src.utils as u
import time
import random
import sys




import time

import logging
TRACE = 5
DEBUG = logging.DEBUG
INFO = logging.INFO

logging.addLevelName(TRACE, "TRACE")
logging.basicConfig(filename='debug.log', level=DEBUG, format='%(asctime)s - %(levelname)s - %(message)s', filemode='w')

logging.debug('Clients number: ' + str(c.num_clients))
logging.debug('Edges number: ' + str(c.num_edges))
logging.debug('Requests number: ' + str(c.req_number))

nodes_thread = []

#Generate threads for each node
for i in range(c.num_edges):
    nodes_thread.append(threading.Thread(target=c.nodes[i].work, daemon=True).start())

start_time = time.time()

job_ids=[]

print(len(c.job_list_instance.job_list))
print('request_number: ' +str(c.req_number))

for job in c.job_list_instance.job_list:
    job_ids.append(job['job_id'])
    for j in range(c.num_edges):
        c.nodes[j].append_data(
            c.message_data
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
                job['read_count']
            )
        )

# Block until all tasks are done.
for i in range(len(c.nodes)):
    c.nodes[i].join_queue()

#Calculate stats
exec_time = time.time() - start_time
logging.info("Run time: %s" % (time.time() - start_time))
print("Run time: %s" % (time.time() - start_time))


time.sleep(1) # Wait time nexessary to wait all threads to finish 

for j in job_ids:
    print('\n')
    print(j)
    logging.info("RESULTS req:" +str(j))
    for i in range(c.num_edges):
        if j not in c.nodes[i].bids:
            print('???????')
            print(str(c.nodes[i].id) + ' ' +str(j))
        # print('ktm')
        print(c.nodes[i].bids[j]['auction_id'])
        # print(c.nodes[i].bids[j]['x'])
        # print(c.nodes[i].initial_cpu)
        # print(c.nodes[i].updated_cpu)
        # print(c.nodes[i].initial_gpu)
        # print(c.nodes[i].updated_gpu)
        # print(c.nodes[i].initial_bw)
        # print(c.nodes[i].updated_bw)

        # print(c.nodes[i].bids)
        # print('id: ' + str(i)+ "avl res: "+ str(c.nodes[i].updated_resources))
        
        # print(vars(c.nodes[i].bids))
        # print(c.nodes[i].__dict__)


# for i in range(c.num_edges):
#     print(c.nodes[i].initial_bw)
#     print(c.nodes[i].updated_bw)


u.calculate_utility(c.nodes, c.num_edges, c.counter, exec_time, c.req_number, job_ids, c.a)

logging.info('Tot messages: '+str(c.counter))
print('Tot messages: '+str(c.counter))

# print(c.t.b)
# print(c.t.call_func())
