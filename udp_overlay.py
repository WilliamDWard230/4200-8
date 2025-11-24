
import base64
import socket
import threading
import time
import zlib
from typing import Tuple

# ---------- Global Configuration ----------
PORT = 5000
BROADCAST_IP = "10.122.255.255"
SYNC_INTERVAL = 5
PING_INTERVAL = 15
PING_TIMEOUT = 3
REMOVE_TIMEOUT = 30


class PeerNode:

    def __init__(self, node_id: str):
        self.id = node_id
        self.ip = None
        self.port = PORT
        self.seq = 0
        self.sock = None
        self.peers = {}
        self.running = False
        self.lock = threading.Lock()

        self._model_buffers = {}  
        self._sent_chunks = {}   

        self._setup_socket()
        self.ip = self._get_local_ip()

    # ---------- Setup ----------
    def _setup_socket(self):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        except Exception:
            pass  # not available on all platforms

        s.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        s.bind(("", PORT))  # listen on all interfaces
        self.sock = s

    def _get_local_ip(self):

        try:
            tmp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

            try:
                tmp.connect(("8.8.8.8", 80))  # no packets actually sent
                ip = tmp.getsockname()[0]
            finally:
                tmp.close()

        except Exception:
            pass
        return "127.0.0.1"


    def _next_seq(self):

        with self.lock:
            self.seq += 1

            return self.seq

    def _make_packet(self, msgtype: str, body: str) -> str:

        ts_ms = int(time.time() * 1000)
        seq = self._next_seq()
        fields = [              #new format provided by shannigrahi
            f"V=1",
            f"SRC={self.id}",
            f"SEQ={seq}",
            f"TYPE={msgtype}",
            f"TS={ts_ms}",
            f"BODY={body}",
        ]
        return "|".join(fields)

    def _send(self, data: str, addr: Tuple[str, int]):
        try:
            self.sock.sendto(data.encode("utf-8"), addr)
        except Exception as e:
            print(f"[ERROR] send to {addr} failed: {e}")

    def broadcast_sync(self):
        body = f"ip={self.ip};port={self.port}"
        pkt = self._make_packet("PEER_SYNC", body)
        self._send(pkt, (BROADCAST_IP, PORT))

    def send_ping(self, peer_id: str, peer_info: dict):
        t0 = int(time.time() * 1000)
        body = f"t0={t0}"
        pkt = self._make_packet("PING", body)
        self._send(pkt, (peer_info["ip"], peer_info["port"]))

    def send_pong(self, addr: Tuple[str, int]):
        # echo original timestamp if present
        body = f"t0={int(time.time() * 1000)}"
        pkt = self._make_packet("PONG", body)
        self._send(pkt, addr)

    def _announce_model_meta(self, ver: str, size: int, chunks: int, sha256_hex: str):
        body = f"ver={ver};size={size};chunks={chunks};sha256={sha256_hex}"
        pkt = self._make_packet("MODEL_META", body)
        self._send(pkt, (BROADCAST_IP, PORT))

    def _send_model_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes, dst: Tuple[str, int]):
        b64 = base64.b64encode(raw_bytes).decode("ascii")
        body = f"ver={ver};idx={idx};total={total};b64={b64}"
        pkt = self._make_packet("MODEL_CHUNK", body)
        self._send(pkt, dst)

    def _cache_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes):
        buf = self._sent_chunks.setdefault(ver, {"total": total, "parts": {}})
        buf["parts"][idx] = raw_bytes
        buf["total"] = total


    def announce_model_meta(self, ver: str, size: int, chunks: int, sha256_hex: str):
        self._announce_model_meta(ver, size, chunks, sha256_hex)

    def send_model_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes, dst: Tuple[str, int]):
        self._cache_chunk(ver, idx, total, raw_bytes)
        self._send_model_chunk(ver, idx, total, raw_bytes, dst)

    def broadcast_model_chunk(self, ver: str, idx: int, total: int, raw_bytes: bytes):
        self._cache_chunk(ver, idx, total, raw_bytes)
        # broadcast to all peers
        self._send_model_chunk(ver, idx, total, raw_bytes, (BROADCAST_IP, PORT))

    # -------------------------------------------------------------------------
    def handle_message(self, msg: str, addr: Tuple[str, int]):
        try:
            parts = msg.strip().split("|")
            kv = {}

            for p in parts:
                if "=" in p:
                    k, v = p.split("=", 1)
                    kv[k] = v

            if kv.get("V") != "1":
                return
            
            src = kv.get("SRC")
            if not src or src == self.id:
                return  # ignore our own packets or malformed
            mtype = kv.get("TYPE", "")
            body = kv.get("BODY", "")
            allowed = {"PEER_SYNC", "PING", "PONG", "MODEL_META", "MODEL_CHUNK", "CHUNK_NAK"}
            if mtype not in allowed:
                return  # ignore foreign/unknown traffic

            # update peer last_seen
            now = time.time()
            with self.lock:
                peer = self.peers.get(src)
                if not peer:
                    self.peers[src] = {
                        "ip": addr[0],
                        "port": addr[1],
                        "last_seen": now,
                        "rtt": None,
                        "model_ver": None,
                        "chunks_recv": None,
                        "chunks_total": None,
                    }
                else:
                    peer["last_seen"] = now
                    peer["ip"], peer["port"] = addr[0], addr[1]

            # handle types
            if mtype == "PEER_SYNC":
                # If we newly heard about this peer, we could respond with a ping to accelerate RTT learning
                pass

            elif mtype == "PING":
                self.send_pong(addr)

            elif mtype == "PONG":
                # compute RTT if they echoed t0
                t0 = None
                for token in body.split(";"):
                    if token.startswith("t0="):
                        try:
                            t0 = int(token.split("=", 1)[1])
                        except Exception:
                            t0 = None
                if t0 is not None:
                    rtt_ms = int(time.time() * 1000) - t0
                    with self.lock:
                        if src in self.peers:
                            self.peers[src]["rtt"] = rtt_ms

            elif mtype == "MODEL_META":
                # track intent to receive model version
                meta = {}
                for token in body.split(";"):
                    if "=" in token:
                        k, v = token.split("=", 1)
                        meta[k] = v
                ver = meta.get("ver")
                total = int(meta.get("chunks", "0") or 0)
                sha = meta.get("sha256", "")
                if ver and total > 0:
                    self._model_buffers.setdefault(
                        ver,
                        {
                            "total": total,
                            "parts": {},
                            "sha256": sha,
                            "sender": src,
                            "last_update": now,
                            "last_nak": 0.0,
                        },
                    )
                    with self.lock:
                        self.peers[src]["model_ver"] = ver
                        self.peers[src]["chunks_recv"] = 0
                        self.peers[src]["chunks_total"] = total
                print(f"[MODEL] announced ver={ver} total={total} from {src}")

            elif mtype == "MODEL_CHUNK":
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
                buf = self._model_buffers.setdefault(
                    ver,
                    {"total": total, "parts": {}, "sha256": "", "sender": src, "last_update": now, "last_nak": 0.0},
                )
                try:
                    buf["parts"][idx] = base64.b64decode(b64.encode("ascii"))
                    buf["last_update"] = now
                    with self.lock:
                        if src in self.peers:
                            self.peers[src]["chunks_recv"] = len(buf["parts"])
                            self.peers[src]["chunks_total"] = total
                    if self.is_model_complete(ver):
                        print(f"[MODEL] complete ver={ver} ({len(buf['parts'])}/{buf['total']} chunks)")
                except Exception as e:
                    print(f"[MODEL] chunk decode error: {e}")
                # (Instructor note) Real implementation would verify SHA256 after reassembly and persist to disk.

            elif mtype == "CHUNK_NAK":
                fields = {}
                for token in body.split(";"):
                    if "=" in token:
                        k, v = token.split("=", 1)
                        fields[k] = v
                ver = fields.get("ver")
                if not ver:
                    return
                missing_field = fields.get("missing", "")
                try:
                    missing = [int(tok.strip()) for tok in missing_field.split(",") if tok.strip().isdigit()]
                except Exception:
                    missing = []
                cache = self._sent_chunks.get(ver, {})
                total = cache.get("total", 0)
                for idx in missing:
                    chunk = cache.get("parts", {}).get(idx)
                    if chunk is not None:
                        self.send_model_chunk(ver, idx, total, chunk, addr)
                        print(f"[NAK] resent chunk {idx} for ver={ver} to {addr}")

            # else: ignore unknown types to preserve extensibility
        except Exception as e:
            print(f"[ERROR] handle_message failed from {addr}: {e}")


    def listener(self):

        while self.running:
            try:
                data, addr = self.sock.recvfrom(65535)
                msg = data.decode("utf-8", errors="ignore")
                self.handle_message(msg, addr)
                
            except OSError:
                break
            except Exception as e:
                print(f"[ERROR] listener: {e}")

    def broadcaster(self):

        while self.running:
            self.broadcast_sync()
            time.sleep(SYNC_INTERVAL)

    def heartbeat(self):

        while self.running:
            now = time.time()
            to_remove = []
            with self.lock:
                items = list(self.peers.items())
            for pid, info in items:
                # ping if due
                if (now - info["last_seen"]) >= PING_INTERVAL:
                    self.send_ping(pid, info)
                # prune if stale
                if (now - info["last_seen"]) >= REMOVE_TIMEOUT:
                    to_remove.append(pid)
            if to_remove:
                with self.lock:
                    for pid in to_remove:
                        self.peers.pop(pid, None)
                        print(f"[PRUNE] removed {pid}")
            time.sleep(1)

    def summary(self):

        while self.running:
            with self.lock:
                peers_copy = dict(self.peers)
            if peers_copy:
                print("\n[SUMMARY] Peers:")
                for pid, p in peers_copy.items():
                    age = int(time.time() - p["last_seen"]) if p.get("last_seen") else -1
                    rtt = p.get("rtt")
                    mv = p.get("model_ver")
                    cr = p.get("chunks_recv")
                    ct = p.get("chunks_total")
                    extra = f" rtt={rtt}ms" if rtt is not None else ""
                    if mv:
                        extra += f" | model={mv} {cr}/{ct}"
                    print(f" - {pid}@{p['ip']}:{p['port']} seen={age}s ago{extra}")
            else:
                print("\n[SUMMARY] No peers known yet.")
            time.sleep(10)

    def retransmit_manager(self):

        while self.running:
            time.sleep(2)
            now = time.time()
            with self.lock:
                buffers = dict(self._model_buffers)
                peers_copy = dict(self.peers)
            for ver, buf in buffers.items():
                total = buf.get("total", 0)
                parts = buf.get("parts", {})
                sender = buf.get("sender")
                last_update = buf.get("last_update", 0)
                last_nak = buf.get("last_nak", 0)

                if not sender or total == 0 or ver not in self._model_buffers:
                    continue
                if self.is_model_complete(ver):
                    continue
                if now - last_update < 3:
                    continue  # give time for natural progress
                if now - last_nak < 3:
                    continue  # avoid spamming
                if sender not in peers_copy:
                    continue

                missing = [i for i in range(total) if i not in parts]
                if not missing:
                    continue

                missing_slice = missing[:25]  # cap request size
                body = f"ver={ver};missing={','.join(str(m) for m in missing_slice)}"
                pkt = self._make_packet("CHUNK_NAK", body)
                peer_addr = (peers_copy[sender]["ip"], peers_copy[sender]["port"])
                self._send(pkt, peer_addr)
                with self.lock:
                    if ver in self._model_buffers:
                        self._model_buffers[ver]["last_nak"] = now
                print(f"[NAK] requested {len(missing_slice)} missing chunks for ver={ver} from {sender}")

    # ---------- Model buffer helpers ----------
    def is_model_complete(self, ver: str) -> bool:
        buf = self._model_buffers.get(ver)
        if not buf:
            return False
        return len(buf["parts"]) == buf.get("total", 0)

    def get_reassembled_model(self, ver: str):
        buf = self._model_buffers.get(ver)
        if not buf or not self.is_model_complete(ver):
            return None
        data = b""
        try:
            for i in range(buf["total"]):
                data += buf["parts"][i]
            return data
        except Exception:
            return None

    # ---------- Control ----------
    def start(self):
        
        if self.running:
            return
        self.running = True
        self._threads = [
            threading.Thread(target=self.listener, daemon=True),
            threading.Thread(target=self.broadcaster, daemon=True),
            threading.Thread(target=self.heartbeat, daemon=True),
            threading.Thread(target=self.summary, daemon=True),
            threading.Thread(target=self.retransmit_manager, daemon=True),
        ]
        for t in self._threads:
            t.start()
        print(f"[START] {self.id} on {self.ip}:{self.port}")

    def stop(self):

        if not self.running:
            return
        self.running = False
        try:
            # poke the socket to unblock recvfrom if needed
            self.sock.settimeout(0.1)
        except Exception:
            pass
        for t in getattr(self, "_threads", []):
            t.join(timeout=1)
        try:
            self.sock.close()
        except Exception:
            pass
        print("[STOP] node stopped")


# ---------- Entry Point ----------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python node_interface.py <node_id>")
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
