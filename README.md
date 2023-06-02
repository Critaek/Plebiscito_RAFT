# Plebiscito 

Plebiscito is a consensus protocol designed for distributed and synchronous resource allocation among a group of nodes. 
Its focus is on allocating High-Performance Machine Learning Tasks (HPMLT).

In Plebiscito, we assume that the ML tasks to be allocated among the nodes consist of Neural Networks (NN). 
To effectively allocate the available resources across the nodes, it may be necessary to split the NN. 
Each NN is characterized by the required percentage of CPU, GPU, and BW for each layer.

When a request for an HPMLT is received, the Plebiscito protocol employs a bidding mechanism to determine the most efficient node for executing the task. 
Multiple messages are exchanged among the nodes until a consensus is reached, ensuring that all nodes are aligned on the winner of the bid for that specific HPMLT.

## RAFT

RAFT is another consensus protocol, designed for distributed and synchronous resource allocation.

In RAFT, we have two different phases, the first one is the leader election, where a leading node is elected. The second phase is the log replication.

RAFT implementation taken from https://github.com/streed/simpleRaft

## Code

To run an experiment:

python main.py 'req_number' 'alpha' 'nodes_number' 'utility_function'

Example:

python main.py 10 1 10 alpha_BW_CPU

- The available utility functions used for the bidding are: "alpha_BW_CPU" "alpha_GPU_BW" "alpha_GPU_CPU"
- The alpha parameter is comprised between 0 and 1 and it is used as a weight in the utility function between the two competing resources.



