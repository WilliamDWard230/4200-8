#!/usr/bin/env python3
"""
UDP Overlay Networking - Sprint 6.2 Format with Model Update Support
-------------------------------------------------------------------
Maintains the original Sprint 6.2 packet format:
[<msgtype>]|<node_id>|<seq>|<timestamp>|<model_ver>|<ttl>|<body>|<crc_hex>

Now includes support for:
- MODEL_META: Announce model delta metadata
- MODEL_CHUNK: Send model delta fragments
"""

import socket
import threading
import time
import struct
import zlib
import base64

# Configuration provided by Shannigrahi
PORT = 5000
BROADCAST_IP = "10.143.255.255"     # Make sure to change the broadcast ip when necessary. ipconfig or ifconfig
SYNC_INTERVAL = 5
PING_INTERVAL = 15
PING_TIMEOUT = 3
REMOVE_TIMEOUT = 30

class PeerNode:

    def __init__(self, node_id: str):
        self.id = node_id       # Node id that is passed in by user 
        self.ip = self._get_local_ip()
        self.port = PORT
        self.seq = 0
        self.sock = None
        self.peers = {}         # Our dict of peers
        self.running = False
        self.lock = threading.Lock()
        self.ping_times = {}
        
        # Add model buffers for storing incoming model chunks
        self._model_buffers = {}  # {ver: {"total": int, "parts": dict[idx->bytes], "sha256": str, "size": int}}
        
        self._setup_socket()
        print(f"[INIT] Node {self.id} initialized at {self.ip}:{self.port}")

    def _setup_socket(self):    # Set up UDP socket
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Enabled SO_REUSEPORT to allow multiple processes on same port
        if hasattr(socket, 'SO_REUSEPORT'):
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.sock.bind(('', self.port))
        self.sock.settimeout(1.0)  # Timeout set to one second

    def _get_local_ip(self):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)    # Get the local ip address for the machine
            s.connect(("8.8.8.8", 80))      # 8.8.8.8 = external
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"      # Set ip to loopback address if attempt failed

    def _next_seq(self):
        with self.lock:
            self.seq += 1
            return self.seq

    def _make_packet(self, msgtype: str, body: str, model_ver: int = 0) -> str:
        timestamp = int(time.time() * 1000)  # UNIX epoch in ms as told by Shannigrahi
        ttl = 3         # Time to live = 3 hops
        seq = self._next_seq()
        
        header_body = f"[{msgtype}]|{self.id}|{seq}|{timestamp}|{model_ver}|{ttl}|{body}"
        
        # Calculate Checksum
        crc = zlib.crc32(header_body.encode()) & 0xffffffff
        crc_hex = format(crc, '08x')        # Hex format
        
        packet = f"{header_body}|{crc_hex}"
        return packet

    def _send(self, data: str, addr: tuple):        # Send UDP pkt to address
        try:
            self.sock.sendto(data.encode(), addr)
        except Exception as e:
            print(f"[ERROR] Failed to send to {addr}: {e}")

    def _verify_crc(self, packet: str) -> bool:         # Validate checksum of entire message
        try:
            parts = packet.rsplit('|', 1)       # Fixed format 
            if len(parts) != 2:
                return False
            
            header_body = parts[0]
            crc_received = parts[1]
            
            crc_calculated = zlib.crc32(header_body.encode()) & 0xffffffff
            crc_hex = format(crc_calculated, '08x')
            
            return crc_hex == crc_received
        except Exception:
            return False

    def broadcast_sync(self):           # Announce yourself to anyone listening with a special PEER_SYNC pkt
        body = f"{self.ip},{self.port}"
        packet = self._make_packet("PEER_SYNC", body)
        self._send(packet, (BROADCAST_IP, self.port))

    def send_ping(self, peer_id: str, peer_info: dict): # Send ping to peer to make sure they are still alive
        timestamp = int(time.time() * 1000)
        body = f"{self.ip},{timestamp}"
        packet = self._make_packet("PING", body)
        
        peer_addr = (peer_info["ip"], peer_info["port"])
        self._send(packet, peer_addr)
        
        # Record ping time for RTT calculation
        with self.lock:
            self.ping_times[peer_id] = timestamp

    def send_pong(self, addr: tuple):   # Respond to ping with pong
        body = f"{self.ip},ok,0"
        packet = self._make_packet("PONG", body)
        self._send(packet, addr)

    # ========== Model Update Helper Methods ==========
    def announce_model_meta(self, ver: str, size: int, chunks: int, sha256_hex: str):
        """Broadcast MODEL_META message to announce a model delta"""
        body = f"ver={ver};size={size};chunks={chunks};sha256={sha256_hex}"
        packet = self._make_packet("MODEL_META", body)
        self._send(packet, (BROADCAST_IP, self.port))
        print(f"[MODEL] Announced model ver={ver}, chunks={chunks}, sha256={sha256_hex[:8]}...")

    def send_model_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes, dst: tuple):
        """Send a MODEL_CHUNK message to a specific destination"""
        b64 = base64.b64encode(raw_bytes).decode("ascii")
        body = f"ver={ver};idx={idx};total={total};b64={b64}"
        packet = self._make_packet("MODEL_CHUNK", body)
        self._send(packet, dst)

    def broadcast_model_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes):
        """Broadcast a MODEL_CHUNK message to all peers"""
        b64 = base64.b64encode(raw_bytes).decode("ascii")
        body = f"ver={ver};idx={idx};total={total};b64={b64}"
        packet = self._make_packet("MODEL_CHUNK", body)
        self._send(packet, (BROADCAST_IP, self.port))

    def is_model_complete(self, ver: str) -> bool:
        """Check if all chunks for a model version have been received"""
        if ver not in self._model_buffers:
            return False
        buf = self._model_buffers[ver]
        return len(buf["parts"]) == buf["total"]

    def get_reassembled_model(self, ver: str) -> bytes:
        """Reassemble all chunks for a model version into complete data"""
        if not self.is_model_complete(ver):
            return None
        
        buf = self._model_buffers[ver]
        data = b""
        for i in range(buf["total"]):
            if i not in buf["parts"]:
                print(f"[MODEL] Missing chunk {i} for version {ver}")
                return None
            data += buf["parts"][i]
        return data

    def handle_message(self, msg: str, addr: tuple):    # Function to parse incoming packets
        try:
            # Verify checksum
            if not self._verify_crc(msg):
                print(f"[CRC] Invalid CRC from {addr}")
                return
            
            # Parse packet
            parts = msg.split('|')
            if len(parts) < 8:
                print(f"[PARSE] Malformed packet from {addr}")
                return
            
            msgtype = parts[0].strip('[]')
            sender_id = parts[1]
            seq_id = parts[2]
            timestamp = int(parts[3])
            model_ver = parts[4]
            ttl = parts[5]
            body = parts[6]
            
            # Ignore own messages
            if sender_id == self.id:
                return
            
            current_time = int(time.time())
            
            # Handle different message types
            if msgtype == "PEER_SYNC":
                body_parts = body.split(',')
                if len(body_parts) == 2:
                    peer_ip = body_parts[0]
                    peer_port = int(body_parts[1])
                    
                    with self.lock:
                        if sender_id not in self.peers:
                            print(f"[SYNC] Added {sender_id} ({peer_ip})")
                        
                        self.peers[sender_id] = {
                            "ip": peer_ip,
                            "port": peer_port,
                            "last_seen": current_time,
                            "status": "active"
                        }
            
            elif msgtype == "PING":
                body_parts = body.split(',')
                if len(body_parts) == 2:
                    # Update peer's last_seen
                    with self.lock:
                        if sender_id in self.peers:
                            self.peers[sender_id]["last_seen"] = current_time
                    
                    # Send PONG response
                    self.send_pong(addr)
            
            elif msgtype == "PONG":
                with self.lock:
                    if sender_id in self.ping_times:
                        sent_time = self.ping_times[sender_id]
                        current_time_ms = int(time.time() * 1000)
                        rtt = current_time_ms - sent_time
                        del self.ping_times[sender_id]
                        
                        if sender_id in self.peers:
                            self.peers[sender_id]["last_seen"] = current_time
                        
                        print(f"[PING] RTT={rtt}ms to {sender_id}")
            
            elif msgtype == "MODEL_META":
                # Parse model metadata announcement
                meta = {}
                for token in body.split(";"):
                    if "=" in token:
                        k, v = token.split("=", 1)
                        meta[k] = v
                
                ver = meta.get("ver")
                total = int(meta.get("chunks", "0") or 0)
                size = int(meta.get("size", "0") or 0)
                sha = meta.get("sha256", "")
                
                if ver and total > 0:
                    # Initialize buffer for this model version
                    self._model_buffers[ver] = {
                        "total": total,
                        "parts": {},
                        "sha256": sha,
                        "size": size,
                        "sender": sender_id
                    }
                    
                    # Update peer info
                    with self.lock:
                        if sender_id in self.peers:
                            self.peers[sender_id]["model_ver"] = ver
                            self.peers[sender_id]["chunks_recv"] = 0
                            self.peers[sender_id]["chunks_total"] = total
                    
                    print(f"[MODEL] Announced ver={ver} total={total} size={size} from {sender_id}")
            
            elif msgtype == "MODEL_CHUNK":
                # Parse model chunk
                fields = {}
                for token in body.split(";"):
                    if "=" in token:
                        k, v = token.split("=", 1)
                        fields[k] = v
                
                ver = fields.get("ver")
                idx = int(fields.get("idx", "-1"))
                total = int(fields.get("total", "0"))
                b64 = fields.get("b64", "")
                
                if ver is None or idx < 0:
                    return
                
                # Initialize buffer if not exists
                if ver not in self._model_buffers:
                    self._model_buffers[ver] = {
                        "total": total,
                        "parts": {},
                        "sha256": "",
                        "size": 0,
                        "sender": sender_id
                    }
                
                buf = self._model_buffers[ver]
                
                try:
                    # Decode and store chunk
                    chunk_data = base64.b64decode(b64.encode("ascii"))
                    buf["parts"][idx] = chunk_data
                    
                    # Update peer info
                    with self.lock:
                        if sender_id in self.peers:
                            self.peers[sender_id]["chunks_recv"] = len(buf["parts"])
                            self.peers[sender_id]["chunks_total"] = total
                    
                    # Check if model is complete
                    if self.is_model_complete(ver):
                        print(f"[MODEL] Completed receiving ver={ver} ({len(buf['parts'])}/{buf['total']} chunks)")
                        # Note: Actual model application would happen in delta_sync.py
                    else:
                        if idx % 10 == 0:  # Print progress every 10 chunks
                            print(f"[MODEL] Chunk {idx}/{total} received for ver={ver}")
                    
                except Exception as e:
                    print(f"[MODEL] Chunk decode error: {e}")
        
        except Exception as e:
            print(f"[ERROR] Failed to handle message from {addr}: {e}")

    def listener(self):
        while self.running:     # Basically while true listen for packets
            try:
                data, addr = self.sock.recvfrom(65535)  # Increased buffer size for MODEL_CHUNK messages
                msg = data.decode()
                self.handle_message(msg, addr)
            except socket.timeout:
                continue
            except Exception as e:
                if self.running:
                    print(f"[ERROR] Listener error: {e}")

    def broadcaster(self):      # Broadcast while running and sleep during interval (5 seconds set at top)
        while self.running:
            self.broadcast_sync()
            time.sleep(SYNC_INTERVAL)

    def heartbeat(self):        # Sleep for interval and then ping (15 seconds set at top)
        while self.running:
            time.sleep(PING_INTERVAL)   # 15s
            
            if not self.running:
                break
            
            current_time = int(time.time())
            peers_to_remove = []
            
            with self.lock:                             # Copy peers list to 
                peers_list = list(self.peers.items())
            
            for peer_id, peer_info in peers_list:       
                # Check if peer has timed out        
                if current_time - peer_info["last_seen"] > REMOVE_TIMEOUT:
                    peers_to_remove.append(peer_id)
                else:
                    # Send ping to active peer
                    self.send_ping(peer_id, peer_info)
            
            # Remove inactive peers
            for peer_id in peers_to_remove:
                with self.lock:
                    if peer_id in self.peers:
                        del self.peers[peer_id]
                        print(f"[DROP] {peer_id} removed (timeout)")

    def summary(self):
        while self.running:
            time.sleep(10)  # Interval for logging updates (changed to 10s for less spam)
            with self.lock:
                active_count = len(self.peers)
                if active_count > 0:
                    print(f"\n[TABLE] {active_count} active peers")
                    for peer_id, info in self.peers.items():
                        age = int(time.time()) - info['last_seen']
                        extra = ""
                        
                        # Add model info if available
                        if "model_ver" in info:
                            mv = info.get("model_ver")
                            cr = info.get("chunks_recv", 0)
                            ct = info.get("chunks_total", 0)
                            extra = f" | model={mv} {cr}/{ct}"
                        
                        print(f"  - {peer_id}: {info['ip']}:{info['port']} (last seen {age}s ago){extra}")
                else:
                    print(f"\n[TABLE] No peers discovered yet")
            
            # Also print model buffer status
            if self._model_buffers:
                print("[MODEL BUFFERS]")
                for ver, buf in self._model_buffers.items():
                    progress = len(buf["parts"])
                    total = buf["total"]
                    sender = buf.get("sender", "unknown")
                    status = "COMPLETE" if progress == total else "IN_PROGRESS"
                    print(f"  - ver={ver}: {progress}/{total} chunks [{status}] from {sender}")

    # Start and stop node
    def start(self):
        self.running = True
        
        # Start the threads
        threading.Thread(target=self.listener, daemon=True).start()
        threading.Thread(target=self.broadcaster, daemon=True).start()
        threading.Thread(target=self.heartbeat, daemon=True).start()
        threading.Thread(target=self.summary, daemon=True).start()
        
        print(f"[START] Node {self.id} started")

    def stop(self):
        print(f"[STOP] Stopping node {self.id}...")
        self.running = False
        time.sleep(2)  # Give the threads time to stop
        
        if self.sock:
            self.sock.close()
        
        print(f"[STOP] Node {self.id} stopped")


# ---------- Entry Point ----------
if __name__ == "__main__":
    import sys
    
    if len(sys.argv) < 2:
        print("Usage: python udp_overlay.py <node_id>")
        exit(1)
    
    node_id = sys.argv[1]
    node = PeerNode(node_id)
    node.start()
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        node.stop()
        print("\n[EXIT] Node stopped.")