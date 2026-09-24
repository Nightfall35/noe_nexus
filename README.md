[README (2).md](https://github.com/user-attachments/files/32631114/README.2.md)
# Node Nexus

Node Nexus is a Java-based distributed-systems demonstration. It models a group of networked nodes that communicate over HTTP, exchange chat messages, elect a coordinator, maintain logical clocks, and circulate a token that protects shared scoreboard updates.

The project includes a Java-served browser dashboard, a terminal control deck, a command-line control client, live node status APIs, election logs, token telemetry, and failover-aware token forwarding.

No Python server, JavaScript package manager, external database, or third-party Java dependency is required.

## Features

- Distributed nodes implemented as independent Java processes.
- HTTP communication using the Java standard library.
- Chat broadcast between configured peers.
- Lamport scalar clocks for event ordering.
- Vector clocks for causal-state tracking.
- Bully-style leader election.
- Leader health checks every five seconds.
- Token-ring mutual exclusion for scoreboard updates.
- Token failover that skips unreachable peers.
- Java-served HTML command center.
- Read-only terminal control deck.
- One-shot command-line control client.
- Human-readable election, token, scoreboard, and status snapshots.
- Support for nodes distributed across multiple computers.

## Requirements

- Java Development Kit 17 or newer.
- Network access between nodes when running across computers.
- Firewall access for the configured node ports.

Verify Java is installed:

```bash
java -version
javac -version
```

## Project layout

```text
Node_Nexus_terminal_deck/
├── dashboard.html
├── README.md
├── src/
│   ├── Node.java
│   ├── Deck.java
│   ├── DeckCtl.java
│   ├── api/
│   │   ├── ChatHandler.java
│   │   ├── NetworkClient.java
│   │   └── StaticDashboardHandler.java
│   ├── models/
│   │   ├── Clock.java
│   │   └── Message.java
│   ├── sync/
│   │   ├── Election.java
│   │   └── MutualExclusion.java
│   └── ui/
│       ├── Dashboard.java
│       ├── EventLog.java
│       └── TypeFX.java
└── out/
    └── compiled Java classes and runtime snapshots
```

## Architecture

Each Java node is a separate process. A node owns its local clock, message log, election state, token state, score map, and HTTP server. Nodes do not share Java memory. They exchange JSON over HTTP.

The default topology contains ten node addresses. The list index is the node ID:

```text
node 0 → localhost:8000
node 1 → localhost:8001
node 2 → localhost:8002
node 3 → localhost:8003
node 4 → localhost:8004
node 5 → localhost:8005
node 6 → localhost:8006
node 7 → localhost:8007
node 8 → localhost:8008
node 9 → localhost:8009
```

The topology is configured in `src/Node.java`:

```java
private static final List<String> PEER_ADDRESSES = Arrays.asList(
    "localhost:8000",
    "localhost:8001",
    "localhost:8002",
    "localhost:8003",
    "localhost:8004",
    "localhost:8005",
    "localhost:8006",
    "localhost:8007",
    "localhost:8008",
    "localhost:8009"
);
```

Every node must use the same ordered peer list. If a node is unavailable, chat and election requests to that node may fail or be ignored. Token forwarding now tries later configured peers and keeps the token if no peer is reachable.

## Build

From the project directory:

```bash
find out -type f -name '*.class' -delete
javac -d out $(find src -name '*.java' -print)
```

The command compiles all source files into the `out` directory.

On Windows PowerShell, compile the Java files under `src` and place the output in `out`:

```powershell
$files = Get-ChildItem -Recurse -Filter *.java src | ForEach-Object { $_.FullName }
javac -d out $files
```

## Run a node

Start one node with a node ID and port:

```bash
java -cp out Node 0 8000
```

The node serves its API and browser dashboard at:

```text
http://localhost:8000/
```

The explicit dashboard URL is:

```text
http://localhost:8000/dashboard.html
```

The Java server locates `dashboard.html` from the project directory, its parent directories, or beside the compiled classes. Python is not required.

## Run the complete local ring

The default setup contains ten nodes. Start them from separate processes:

```bash
for i in $(seq 0 9); do
  java -cp out Node "$i" "$((8000 + i))" > "node${i}.log" 2>&1 &
done
```

For the most predictable token demonstration, start all configured nodes before requesting score updates.

To stop the local nodes on Linux or macOS:

```bash
pkill -f 'java.*Node'
```

On Windows PowerShell, stop the Java processes running `Node` from Task Manager or use a process query appropriate for your environment.

## Two-node testing

The current failover implementation allows a partial ring to communicate. For example, nodes 0 and 2 can run while nodes 1 and 3–9 are offline:

```bash
java -cp out Node 0 8000
java -cp out Node 2 8002
```

Node 0 attempts node 1 first, skips it when it is unreachable, and can deliver the token to node 2. Node 2 then searches for a reachable successor and can return the token to node 0.

For a cleaner two-node demonstration, use a two-entry peer list in `Node.java` and run node IDs 0 and 1 on the selected ports:

```java
private static final List<String> PEER_ADDRESSES = Arrays.asList(
    "localhost:8000",
    "localhost:8002"
);
```

Then run:

```bash
java -cp out Node 0 8000
java -cp out Node 1 8002
```

Recompile after changing the peer list.

## Browser dashboard

Start at least one node and open:

```text
http://localhost:8000/
```

The dashboard provides:

- Online and offline node counts.
- Current leader.
- Token holder.
- Token pass count.
- Animated SVG token topology.
- Token movement and failover state.
- Live merged chat stream.
- Scoreboard view.
- Election and health activity.
- Chat, score, and election controls.

The dashboard polls `/api/status` once per second. Use the sidebar to configure the host, starting port, and node count.

When nodes run on another computer, open the dashboard using that computer's address:

```text
http://192.168.1.10:8000/
```

The node server binds to all network interfaces. The firewall must allow the selected port.

## Terminal control deck

`Deck` is a read-only terminal dashboard that polls all configured node status endpoints:

```bash
java -cp out Deck
```

Optional arguments:

```bash
java -cp out Deck <host> <startPort> <count>
```

Example:

```bash
java -cp out Deck 192.168.1.10 8000 4
```

The deck displays node health, leader state, token state, token successor, pass count, Lamport time, vector clocks, scoreboard data, chat, and recent events.

## Command-line controls

`DeckCtl` sends one command to one node and exits.

### Send chat

```bash
java -cp out DeckCtl localhost:8000 send "hello from node 0"
```

### Request a score update

```bash
java -cp out DeckCtl localhost:8000 score 5
```

The delta is applied when the target node receives the token.

### Force an election

```bash
java -cp out DeckCtl localhost:8000 elect
```

## Token-ring behavior

The token controls access to the scoreboard. Only the node holding the token can apply a queued score update.

A score request follows this flow:

```text
1. Queue the requested score delta.
2. Check whether the node owns the token.
3. If not, wait for the token.
4. When the token arrives, merge its scoreboard.
5. Apply the local score delta.
6. Forward the token to the next reachable peer.
```

Token states include:

```text
HOLDING
RECEIVED
IN_FLIGHT -> host:port
DELIVERED -> host:port
STALLED -> no reachable peer
```

If the immediate successor is offline, the token controller tries later addresses from the configured ring. If all peers fail, the current node retains the token rather than losing it.

## Leader election

The project uses a simplified Bully-style algorithm. Each node has a numeric ID, and higher IDs have priority.

When a node starts an election, it contacts every higher-ID peer. If a higher node responds, that node contests the election. The highest reachable node eventually receives no response from a higher ID and declares itself leader. It then broadcasts a coordinator message.

Non-leaders check the current leader's `/api/health` endpoint every five seconds. A failed health check starts a new election.

Leader election and token ownership are separate. The leader does not automatically own the scoreboard token.

## API reference

| Endpoint | Method | Description |
|---|---:|---|
| `/api/health` | GET | Returns a basic liveness response. |
| `/api/status` | GET | Returns node, leader, clock, token, score, and message state. |
| `/api/messages` | GET | Returns the local chat message log. |
| `/api/chat` | POST | Receives a peer chat message. |
| `/api/send` | POST | Creates and broadcasts a local chat message. |
| `/api/token` | POST | Receives the token and scoreboard payload. |
| `/api/election` | POST | Receives election and coordinator messages. |
| `/api/action/score` | POST | Queues a scoreboard delta and requests the token. |
| `/api/action/elect` | POST | Starts an election immediately. |

Health check:

```bash
curl http://localhost:8000/api/health
```

Expected response:

```json
{"status":"ALIVE"}
```

Status check:

```bash
curl http://localhost:8000/api/status
```

Useful status fields include:

| Field | Meaning |
|---|---|
| `node_id` | Numeric node identity. |
| `leader_id` | Node currently believed to be leader. |
| `has_token` | Whether this node owns the token. |
| `token_state` | Current token movement state. |
| `token_next_peer` | Current successor candidate. |
| `token_passes` | Number of token forwarding attempts. |
| `scores` | Locally known scoreboard. |
| `messages` | Locally known chat messages. |

## Two-computer deployment

Assume:

| Computer | Address | Nodes |
|---|---|---|
| PC A | `192.168.1.10` | nodes 0–4 on ports 8000–8004 |
| PC B | `192.168.1.20` | nodes 5–9 on ports 8005–8009 |

Replace the peer list in `Node.java` on both computers:

```java
private static final List<String> PEER_ADDRESSES = Arrays.asList(
    "192.168.1.10:8000",
    "192.168.1.10:8001",
    "192.168.1.10:8002",
    "192.168.1.10:8003",
    "192.168.1.10:8004",
    "192.168.1.20:8005",
    "192.168.1.20:8006",
    "192.168.1.20:8007",
    "192.168.1.20:8008",
    "192.168.1.20:8009"
);
```

Both computers must use the exact same list. Recompile on both computers.

Start the nodes on PC A:

```bash
for i in $(seq 0 4); do
  java -cp out Node "$i" "$((8000 + i))" > "node${i}.log" 2>&1 &
done
```

Start the nodes on PC B:

```bash
for i in $(seq 5 9); do
  java -cp out Node "$i" "$((8000 + i))" > "node${i}.log" 2>&1 &
done
```

Test each direction:

```bash
curl http://192.168.1.20:8005/api/health
curl http://192.168.1.10:8000/api/health
```

Both commands should return:

```json
{"status":"ALIVE"}
```

Allow the node ports through both operating systems' firewalls. Do not use `localhost` in the cross-computer peer list because `localhost` refers to the computer making the request.

## Troubleshooting

### `java.net.ConnectException`

The destination address refused the connection. Check that the target node is running, the IP and port are correct, the firewall allows the port, and both machines use the same peer list.

### Token state is `STALLED`

`STALLED` means the token controller could not reach any configured peer. Check the health endpoints of the other nodes:

```bash
curl http://localhost:8000/api/health
curl http://localhost:8002/api/health
```

Also confirm that the updated classes were compiled after extracting the project.

### The browser dashboard is blank or missing

Confirm that a Java node is running and open both URLs:

```text
http://localhost:8000/
http://localhost:8000/dashboard.html
```

Recompile if using an older archive:

```bash
find out -type f -name '*.class' -delete
javac -d out $(find src -name '*.java' -print)
```

### Multiple leaders appear

Check that every node uses the same peer list and that election requests are not blocked by a firewall. A network partition can cause separate groups to elect different leaders because this demonstration does not implement quorum consensus.

### Network diagnostics

Chat connection errors are quiet by default. Enable detailed network logging with:

```bash
java -Dnode.debug.network=true -cp out Node 0 8000
```

## Runtime files

The node writes several runtime files in its working directory:

| File | Purpose |
|---|---|
| `status_node<id>.txt` | Human-readable local status view. |
| `election_node<id>.log` | Append-only election history. |
| `token_node<id>.log` | Current token state and successor. |
| `scoreboard_node<id>.log` | Current local scoreboard snapshot. |
| `node<id>.log` | Optional shell redirection of console output. |

These files are diagnostic snapshots. They can be deleted before a clean run.

## Design limitations

Node Nexus is an educational distributed-systems demonstration, not a production consensus system. It does not provide:

- Authentication or authorization.
- TLS encryption.
- Durable database storage.
- Membership discovery.
- Quorum consensus.
- Automatic topology repair.
- Durable recovery after process crashes.
- Strict schema validation for all JSON input.

The JSON parsing and serialization are intentionally lightweight and use regular expressions and string construction. The peer topology is configured in source code. These choices reduce setup complexity while making the implementation easier to study.

## License

Add the license appropriate for your project before publishing this repository.
