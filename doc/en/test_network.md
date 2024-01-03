# Start the Test Network

This section will discuss how to test a network on a single host or in a distributed deployment.
By deploying a test network, developers can verify the correctness of smart contracts and configuration files.

## Deploying on a Single Host

### Prepare config file

You can generate a configuration file template using a Certificate Authority (CA),
or you can use the following sample configuration file for a group of three nodes:

```yaml
chaincode:
  ycsb:
    init: true
  small_bank:
    init: true
nodes:
  group_0:
    - node_id: 0
      group_id: 0
      ski: 0_0
      pri_ip: 127.0.0.1
      pub_ip: 127.0.0.1
    - node_id: 1
      group_id: 0
      ski: 0_1
      pri_ip: 127.0.0.1
      pub_ip: 127.0.0.1
    - node_id: 2
      group_id: 0
      ski: 0_2
      pri_ip: 127.0.0.1
      pub_ip: 127.0.0.1
  local_group_id: 0
  local_node_id: 0
bccsp:
  0_0:
    raw: 0b8d3a7cee7686d9deda24fd5e0dcd9d6e3e9fe6a14cd56cf5f44c86943614eb
    private: true
  0_1:
    raw: 1b79eb109cdc403cd5e42e3c6d052a64cc0d28d5d148757a68ec7004b1fbfdfd
    private: true
  0_2:
    raw: 4dfdc20098dfff76d5ed9555b1786fb0b6b173b8bfe7273ba44869bab36faaf5
    private: true
aria_worker_count: 1
bccsp_worker_count: 1
batch_max_size: 100
batch_timeout_ms: 100
distributed: false
ssh_username: user
ssh_password: 123456
running_path: /root/nc_bft/
jvm_path: /root/nc_bft/corretto-16.0.2/bin/java
replicator_lowest_port: 19990
small_bank:
  target_throughput: 2000
  thread_count: 1
tpcc:
  target_throughput: 2000
  thread_count: 1
  warehouse_count: 5
ycsb:
  target_throughput: 2000
  thread_count: 1
  record_count: 1000000
  read_proportion: 0.5
  update_proportion: 0.5
```

#### Note:
* change "running_path" to BFT-SMART folder
* change "jvm_path" to corretto in the nc_bft release
* change "ssh_username" to your username
* change "ssh_password" to your password
* make sure your host can be accessed via ssh at default port

Please name the configuration file as "peer.yaml" and place it in the execution path of the peer binary for it to be loaded.

```sh
cd mass_bft/build/standalone
vi peer.yaml
```

### Init the database

Execute the following three commands in separate terminals to init the database of the three nodes.

```shell
./peer -i=ycsb -n=0
```
```shell
./peer -i=ycsb -n=1
```
```shell
./peer -i=ycsb -n=2
```

### Start the nodes

If a non-volatile database (i.e., not a hash table) is used, you need to execute the following command to start the node.
Otherwise, the node will automatically start after initializing the database.

```shell
./peer -n=0
```
```shell
./peer -n=1
```
```shell
./peer -n=2
```

### Test the network

You can use the SDK to develop your own client for testing the network
(which requires initializing the corresponding database using "./peer -i=[your chaincode name]"),
or you can use the YCSB benchmark to test the network.

```shell
./ycsb
```

It is important to note that by default, BFT-SMaRt designates Node 0 as the leader node.
Therefore, user transactions must be sent to Node 0 for processing.
Before executing YCSB, ensure that the client sends transactions to Node 0 by modifying the following lines in the "peer.yaml" configuration file.

```yaml
nodes:
  local_node_id: 0
```

### Clean the environment
```shell
cd /root/nc_bft
rm -r data
rm -f *.txt
```

### Troubleshooting

#### The port is already in use.

When executing the peer, if it prompts that the port is already in use, it may be because the system is using a range of ports, such as 51200-51500.
It is difficult to guarantee that other applications (such as browsers) will not occupy any of these ports.
Therefore, when encountering a port conflict, please change the port by modifying the "replicator_lowest_port" parameter in the configuration file.

During single-machine testing, it is also possible that you have not modified the "distributed" option in the configuration file to "false".
In distributed testing, each peer listens on the same port to reduce the likelihood of the issue mentioned in the previous paragraph.
However, during single-machine testing, since a port can only be listened to once, different peers will listen on different ports.

#### The system throughput is 0

The peer can only function properly after displaying the message "This peer is started." If there are issues with your network, this line may not be displayed.
The problem may be related to the BFT or Raft consensus module.

## Distributed Deployment

Similar to single-machine deployment, you only need to modify the corresponding IP addresses to the actual IP addresses of the machines.
These machines should be accessible for SSH password login and have the same username and password.