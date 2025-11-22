# CSC4200 – Assignment 8

## Distributed Model Delta Synchronization and Overlay Networking

### **README**

---

## 1. Overview

In this assignment, you will deploy a distributed learning node on your Raspberry Pi.
Each Raspberry Pi will run a lightweight language model, participate in a UDP-based peer-to-peer overlay, and exchange model updates with other nodes on the local network.

Your primary responsibility is to implement the **networking mechanisms** that enable model deltas to be transmitted, received, verified, and merged.
The machine-learning components are provided for you and must not be modified.

This assignment builds directly on the previous overlay labs and extends your system into a federated learning environment. Use the message format from your previous assignment (Sprint 6.2).

Here is how it should work:

```
prompt_node.py
    ↓
delta_sync.py
    ↓
update_exchanges.py
    ↓
udp_overlay.py
```

## Chaining of files/modules/functions

# Dependency Chain

```text
prompt_node.py
    imports udp_overlay.PeerNode, BROADCAST_IP, PORT
    imports delta_sync.export_delta, broadcast_delta, apply_incoming_deltas

delta_sync.py
    imports udp_overlay.PeerNode, BROADCAST_IP, PORT
    imports update_exchanges.announce_model_meta
    imports update_exchanges.fragment_and_send
    imports update_exchanges.handle_incoming_chunk
    imports update_exchanges.receive_and_reassemble

update_exchanges.py
    imports udp_overlay.PeerNode, BROADCAST_IP, PORT

udp_overlay.py
    imports base libs only (base64, socket, threading, time, typing)
```

---

## 2. Required Model

All nodes must use the same pre-trained model:

**`MODEL_NAME = "sshleifer/tiny-gpt2"`**

This model is small enough to run efficiently on a Raspberry Pi and produces update files that are manageable over UDP.
You must ensure that this exact model name is used in the provided `prompt_node.py` file.
Using any other model will cause inconsistent behavior across nodes and will result in a loss of marks.

---

## 3. Repository Contents

Your repository contains several files that work together to create the full system.

### 3.1 `prompt_node.py`

This file loads the model, manages user interaction, initiates local fine-tuning when the user provides corrected answers, and triggers delta generation.
It also calls your networking functions to broadcast and merge model updates.
You should **not** modify this file.

### 3.2 `udp_overlay.py`

This file implements the decentralized UDP overlay.
It provides peer discovery, heartbeat messages, message parsing, and message routing.
It also defines the networking API that your delta synchronization code must use.
You must not alter any of its control flow, discovery logic, or threading behavior.
Your code must integrate cleanly with its provided interfaces.

### 3.3 `delta_sync.py`

This file is the focus of the assignment.
It contains:

* A provided function that computes model deltas (you do not modify this).
* Two functions that **you must complete**:

  * `broadcast_delta()`
  * `apply_incoming_deltas()`

These functions implement the networking portions of model-delta synchronization, including fragmentation, announcement, transmission, reassembly, verification, and merging.

---

## 4. Setting Up the Raspberry Pi

To begin, connect to your Raspberry Pi via SSH:

```
ssh pi@<pi-address>
```

Update your system and install Python dependencies:

```
sudo apt update
sudo apt install python3-pip -y
pip install torch transformers
```

Confirm that the model name in `prompt_node.py` is set to:

```
MODEL_NAME = "sshleifer/tiny-gpt2"
```

All nodes must use this exact model.

---

## 5. Running a Node

Launch a node using:

```
python3 prompt_node.py Pi-1
```

Use a distinct identifier for each Raspberry Pi (e.g., `Pi-2`, `Pi-3`).
Once running, the node will automatically join the overlay, announce its presence, and listen for model-delta updates.

---

## 6. How the Overlay Works

When a node starts:

1. It broadcasts periodic presence messages so that other nodes can discover it.
2. It listens for incoming `MODEL_META` and `MODEL_CHUNK` messages.
3. When a node trains on new data, it computes a model delta file.
4. Your implementation of `broadcast_delta()` must send this delta over the overlay in safe-sized UDP fragments.
5. Other nodes receive these fragments and pass them to your `apply_incoming_deltas()` implementation.
6. Once all fragments of a particular delta version have been received, your code must reassemble the update, verify its SHA-256 hash, and merge it into the model.

The rest of the overlay logic is handled entirely by `udp_overlay.py`.

---

## 7. Your Responsibilities

You are responsible for implementing two functions in `delta_sync.py`:

### 7.1 `broadcast_delta(node, path, sha, size)`

You must:

* Create a version identifier.
* Compute the number of UDP-safe fragments needed for the delta file.
* Announce the delta to all peers using the provided metadata function.
* Use message format from Sprint 6.2.
* Fragment the binary delta file into pieces that fit within the overlay’s maximum payload size.
* Base64-encode each fragment so that it can be transmitted as text.
* Send each fragment using the overlay’s `_send_model_chunk()` interface.
* Produce clear and informative log messages showing progress.

### 7.2 `apply_incoming_deltas(node, model, merge_weight=1.0)`

You must:

* Identify delta versions for which all fragments have been received.
* Reassemble the original binary data in the correct order.
* Verify that the SHA-256 hash of the reassembled file matches the metadata.
* Load the delta using the provided ML utilities.
* Add each delta tensor to the model parameters.
* Save the updated model state as the new baseline.
* Produce informative log messages showing which updates were applied.

These two functions determine your grade for this assignment.

---

## 8. Testing Your Implementation

To test your system:

1. Start at least two Raspberry Pis on the same network.
2. Launch one node as `Pi-1` and another as `Pi-2`.
3. Query `Pi-1` with any question.
4. Provide a corrected answer when prompted, which will trigger local training.
5. Observe whether `Pi-1` broadcasts the delta in multiple fragments.
6. Observe whether `Pi-2` receives, reassembles, verifies, and merges the delta.
7. Query `Pi-2` to determine whether the learning has synchronized correctly.

You should see confirmation messages during every stage of the process.

---

## 9. Submission Instructions

Submit the following items through GitHub Classroom:

* Your completed `delta_sync.py` file.
* Description of who worked on which section of the project.
* A short reflection describing what you implemented and what challenges you encountered.

---

## 10. Bonus Credit (Up to +5 Points)

You may earn bonus points if:

* Your node consistently produces correct answers after training.
* The distributed system remains stable across three or more Pis.
* You improve answer quality or reduce repetition during generation.
* You provide meaningful insights or optimizations in your reflection.

Submit supporting screenshots or experimental notes to receive credit.

