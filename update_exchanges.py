
import base64
import hashlib
import os
from typing import Tuple

MAX_CHUNK_SIZE = 970  #ensure that with base 64 expansion and header that packet is below mtu


def announce_model_meta(node, ver: str, size: int, chunks: int, sha: str):

    node.announce_model_meta(ver, size, chunks, sha)
    print(f"[DELTA] Announced model meta: ver={ver}, size={size}, chunks={chunks}")


def fragment_and_send(node, ver: str, filepath: str, addr: Tuple[str, int] = None):
    # verify file
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return 0
    
    # Read the file and calculate hash
    with open(filepath, "rb") as f:
        data = f.read()
    
    file_size = len(data)
    sha256_hash = hashlib.sha256(data).hexdigest()
    
    
    raw_chunk_size = int(MAX_CHUNK_SIZE * 3 / 4) 
    total_chunks = (file_size + raw_chunk_size - 1) // raw_chunk_size
    
    print(f"[DELTA] Fragmenting {filepath}: {file_size} bytes into {total_chunks} chunks")
    
    announce_model_meta(node, ver, file_size, total_chunks, sha256_hash)
    
    for idx in range(total_chunks):
        start = idx * raw_chunk_size
        end = min(start + raw_chunk_size, file_size)
        chunk_data = data[start:end]
        
        # send/broadcast chunks
        if addr:
            node.send_model_chunk(ver, idx, total_chunks, chunk_data, addr)
        else:
            node.broadcast_model_chunk(ver, idx, total_chunks, chunk_data)
        
        # progress log for sent chunks
        if idx % 10 == 0 or idx == total_chunks - 1:
            print(f"[DELTA] Sent chunk {idx + 1}/{total_chunks} for ver={ver}")
    
    print(f"[DELTA] Completed sending {total_chunks} chunks for ver={ver}")
    return total_chunks


def handle_incoming_chunk(node, msg: str, addr: Tuple[str, int]):

    # parse incoming messages
    try:
        parts = msg.split('|')
        if len(parts) < 7:
            return False
        
        body = parts[6]
        fields = {}
        for token in body.split(";"):
            if "=" in token:
                k, v = token.split("=", 1)
                fields[k] = v
        
        # extract version
        ver = fields.get("ver")
        if not ver:
            return False
        
        if node.is_model_complete(ver):
            print(f"[DELTA] Model ver={ver} is complete, ready for processing")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Failed to process incoming chunk: {e}")
        return False


def receive_and_reassemble(node, ver: str, save_path: str = None) -> str:

    if not node.is_model_complete(ver):
        
        print(f"[DELTA] Model ver={ver} is not complete yet")
        return None
    
    # Get reassembled data
    data = node.get_reassembled_model(ver)

    if data is None:

        print(f"[ERROR] Failed to reassemble model ver={ver}")
        return None
    
    # Verify hash if available
    buf = node._model_buffers.get(ver, {})
    expected_sha = buf.get("sha256", "")

    if expected_sha:
        actual_sha = hashlib.sha256(data).hexdigest()

        if actual_sha != expected_sha:
            print(f"[ERROR] Hash mismatch for ver={ver}!")
            print(f"  Expected: {expected_sha}")
            print(f"  Actual:   {actual_sha}")
            return None
        
        print(f"[DELTA] Hash verified for ver={ver}")
    
    # Save to file
    if not save_path:
        save_path = f"/tmp/model_delta_{ver}.bin"
    
    with open(save_path, "wb") as f:
        f.write(data)
    
    print(f"[DELTA] Saved reassembled model to {save_path} ({len(data)} bytes)")
    
    # Clean up buffer to save memory
    if ver in node._model_buffers:
        del node._model_buffers[ver]
    
    return save_path


def get_pending_models(node) -> list:
    
    # build list of incomplete versions
    pending = []

    for ver, buf in node._model_buffers.items():
        progress = len(buf["parts"])
        total = buf["total"]

        if progress < total:
            pending.append((ver, progress, total))

    return pending


def get_complete_models(node) -> list:

    # build list of complete versions
    complete = []

    for ver in node._model_buffers:
        if node.is_model_complete(ver):
            complete.append(ver)

    return complete


# ========== Testing Functions (Not for production) ==========
if __name__ == "__main__":
    print("This module should be imported and used with delta_sync.py")
    print("The functions here are designed to work with the PeerNode class")
    print("from udp_overlay.py in a distributed environment.")
    print()
    print("Key functions:")
    print("  - announce_model_meta(): Announce model metadata to peers")
    print("  - fragment_and_send(): Fragment and send a model delta file")
    print("  - handle_incoming_chunk(): Process incoming MODEL_CHUNK messages")
    print("  - receive_and_reassemble(): Reassemble a complete model from chunks")
    print("  - get_pending_models(): Check incomplete model transfers")
    print("  - get_complete_models(): Get list of complete models ready for processing")