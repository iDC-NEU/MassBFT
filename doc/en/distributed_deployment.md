# Automated Distributed Deployment

Automated distributed deployment requires the utilization of a Certificate Authority (CA).
The CA efficiently generates corresponding configuration files based on the desired number of target nodes.
These files with the source codes are then automatically compiled, deployed, and executed on remote peers.

## Preparing configuration files

In order to generate accurate configuration files, it is necessary to determine the number of clusters
within the system and the quantity of nodes present within each cluster.

Please confirm that all machines are accessible via SSH and have the same username and password

We assume that the CA and peer share the same storage location for the source code,
which is `_runningPath` (default is `/home/user`).
The compressed package for PBFT-related components is named `nc_bft.zip`,
and the compressed package for the system source code is named `ncp.zip`.
The `nc_bft.zip` package includes the Clash component (configuration file `proxy.yaml`),
which improves GitHub connection quality during compilation.
Corretto 16.0.2 provides the Java runtime environment and PBFT components (`nc_bft.jar` and configuration files).
The peer utilizes this directory as its default runtime directory.
If the directory doesn't exist, the peer will raise a `path_not_found` exception and exit.

### 1. Compile and launch the CA backend.

You can utilize the following code to compile and run the CA:

```shell
cmake -DCMAKE_BUILD_TYPE=Release -DCMAKE_C_COMPILER=/usr/bin/gcc-11 -DCMAKE_CXX_COMPILER=/usr/bin/g++-11 -B build
cmake --build build --target NBPStandalone_ca -j
./build/standalone/ca -r=/root
```

You can also prepend parameters before running the CA to override default values (modifications through configuration files for overrides are not supported).

For example:
```shell
./build/standalone/ca -r=/tmp -b=bft -n=sc
```

The CA backend listens on port 8081. Without utilizing the graphical frontend, you only need to send a POST request
to the corresponding URL with parameters in JSON format.
You can employ software like Postman or Fiddler to generate the appropriate requests.

### 2. Configure the number of clusters and the number of nodes within each cluster.

Utilize suitable software (or a graphical frontend) to set the quantity of nodes within each cluster (here demonstrated using Postman as an example).

For instance, if you wish to create a system with 3 clusters, each containing 4 nodes,
you can populate the request body with the following content: `[4, 4, 4]`

Send the request to `${ca_server_ip}:8081/config/init`.
If the returned JSON contains the "success" field, it indicates the successful execution of the request.
You can also monitor the execution status through the CA backend console.

### 3. Configure the IP address of each node.

You can invoke `${ca_server_ip}:8081/config/node` to configure the IP address for a single node at a time,
or use `${ca_server_ip}:8081/config/nodes` to configure IP addresses for multiple nodes in a single operation.

For example: `${ca_server_ip}:8081/config/node`
```json
{"group_id": 0, "node_id": 0, "pub": "127.0.0.1", "pri": "127.0.0.1", "is_client": false}
```

For example: `${ca_server_ip}:8081/config/nodes`
```json
[
  {"group_id": 0, "node_id": 0, "pub": "47.74.19.210", "pri": "172.21.20.62", "is_client": false},
  {"group_id": 0, "node_id": 1, "pub": "47.245.0.102", "pri": "172.21.20.63", "is_client": false},
  {"group_id": 0, "node_id": 2, "pub": "47.91.11.55", "pri": "172.21.20.60", "is_client": false},
  {"group_id": 0, "node_id": 3, "pub": "47.245.12.167", "pri": "172.21.20.64", "is_client": false},
  {"group_id": 0, "node_id": 0, "pub": "47.245.0.8", "pri": "172.21.20.61", "is_client": true}
]
```

It's worth noting that each cluster requires a client to send messages; otherwise, it might lead to system blockage.
Furthermore, due to PBFT's default usage of the node with index 0 as the leader,
the client must be configured to send messages to the 0th node of the cluster, ensuring optimal performance.


Set the username and password of remote nodes:

For example: `${ca_server_ip}:8081/peer/config`
```json
{
    "parent": "",
    "key": "ssh_username",
    "value": "root"
}
```

For example: `${ca_server_ip}:8081/peer/config`
```json
{
    "parent": "",
    "key": "ssh_password",
    "value": "1_ssh_Password."
}
```

### 4. Upload the source code to remote nodes.

prepare ncp.zip
```shell
cd ~  # in the parent path of mass_bft
rm -rf ncp
cp -r mass_bft ncp
rm -rf ncp/build
zip -r -q ncp.zip ncp
```

prepare nc_bft.zip (skip if you have already downloaded it)
```shell
cd ~
wget https://github.com/iDC-NEU/mass_bft/releases/download/dep/nc_bft.zip
```

```
root@iZ6weg2bv7ohyev6mlnzdsZ:~# tree -L 1
.
├── mass_bft
├── nc_bft
├── nc_bft.zip
├── ncp
├── ncp.zip
└── snap
```

After ensuring that `$(running_path)/nc_bft.zip` and `$(running_path)/ncp.zip` are prepared, you can use the following RPC to instruct the peer to compile the system:

URL: `${ca_server_ip}:8081/upload/all`
```json
[]
```

By employing an empty body, the CA will initialize all nodes. If the body is a list, the CA will solely initialize the nodes within the list.
```json
[ "47.92.126.166", "39.100.65.141" ]
```

### 5. Compile the source code.
URL: `${ca_server_ip}:8081/upload/compile`
```json
[]
```
During the build process, dependencies corresponding to GitHub repositories need to be downloaded. The download speed relies on the network environment.
You can configure Clash to expedite downloads or include the `build` folder in `ncp.zip` after pre-downloading it through CA.

Please note that when `ncp.zip` includes the `build` folder,
ensure that the absolute paths of the `ncp` folders for both the CA and peer are identical.
In other words, the `build` folder should reside within the `/home/user/ncp` folder.

### 6. Upload config file for each node.
The configuration file encompasses the following sections:

1. System configuration for the blockchain, such as block generation time, maximum block size, thread pool count, and more.
2. Configuration for the workload, including the thread pool count for generating workloads by clients, specific workload configurations, and so on.
3. Node information, such as public and private keys for each node, which must remain consistent across all nodes.
Nodes only require their own private key, so you can remove other nodes' private keys in the configuration file.

URL: `${ca_server_ip}:8081/upload/prop`
```json
[]
```

### 7. Init workload.

To execute a workload, you need to generate the corresponding database for that workload.

URL: `${ca_server_ip}:8081/peer/generate`
```json
{
  "name": "ycsb"
}
```

Note that if the database employs a hashmap, the peer will not exit after generating the database.

### 8. Start peer.

URL: `${ca_server_ip}:8081/peer/start`
```json
[]
```

### 9. Start user.

URL: `${ca_server_ip}:8081/user/start`
```json
[]
```

#### Other workload

If you want to use `${ca_server_ip}:8081/peer/generate` to generate TPC-C database,
please use the following request body:
```json
{ "name": "tpcc" }
```

For small bank:

```json
{ "name": "sb" }
```

To start the user with `${ca_server_ip}:8081/user/start`, if you are using TPC-C chaincode:

```json
{ "name": "tpcc" }
```

For small bank:

```json
{ "name": "small_bank" }
```

### Troubleshooting
