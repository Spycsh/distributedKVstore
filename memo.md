# Memo
## Basic Introduction
This is a key - value store system. There are three operations: get, put and cas. My implementation 
uses Sequence Paxos with Gossip Leader Election for
 synchronizing the replicas. Currently there is no
 Failure detector or Broadcast component. 

current running: 3 server with 1 client - ok

## To do
- [x] connecting the component of SC and BLE
- [ ] Delete Route usage in the OpsTest
- [ ] what if 1 server fails
- [ ] test