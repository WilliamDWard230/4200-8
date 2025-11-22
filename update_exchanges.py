#!/usr/bin/env python3
"""
Model Update Exchanges - Sprint 6.2 Compatible
----------------------------------------------
Functions for fragmenting, sending, and reassembling model deltas
using the Sprint 6.2 packet format with CRC checksums.

These functions are designed to be called from delta_sync.py
"""

import base64
import hashlib
import os
from typing import Tuple

# Maximum payload size for UDP (conservative to fit within MTU)
# Sprint 6.2 format adds overhead: [TYPE]|ID|SEQ|TS|VER|TTL|BODY|CRC
# Estimated overhead ~200 bytes, so we use 970 for base64 encoded data
MAX_CHUNK_SIZE = 970  # Base64 encoded size per chunk


def announce_model_meta(node, ver: str, size: int, chunks: int, sha: str):
    """
    Announce model metadata to all peers.
    
    Args:
        node: PeerNode instance
        ver: Version string for the model delta
        size: Total size of the delta file in bytes
        chunks: Number of chunks the file is split into
        sha: SHA256 hash of the complete file (hex string)
    """
    node.announce_model_meta(ver, size, chunks, sha)
    print(f"[DELTA] Announced model meta: ver={ver}, size={size}, chunks={chunks}")


def fragment_and_send(node, ver: str, filepath: str, addr: Tuple[str, int] = None):
    """
    Fragment a model delta file and send it as chunks.
    
    Args:
        node: PeerNode instance
        ver: Version string for this delta
        filepath: Path to the delta file
        addr: Destination address (if None, broadcasts to all peers)
    
    Returns:
        Number of chunks sent
    """
    if not os.path.exists(filepath):
        print(f"[ERROR] File not found: {filepath}")
        return 0
    
    # Read the file and calculate hash
    with open(filepath, "rb") as f:
        data = f.read()
    
    file_size = len(data)
    sha256_hash = hashlib.sha256(data).hexdigest()
    
    # Calculate number of chunks needed
    # Each chunk will be base64 encoded, which increases size by ~33%
    # So we need to work with smaller raw chunks
    raw_chunk_size = int(MAX_CHUNK_SIZE * 3 / 4)  # Reverse of base64 expansion
    total_chunks = (file_size + raw_chunk_size - 1) // raw_chunk_size
    
    print(f"[DELTA] Fragmenting {filepath}: {file_size} bytes into {total_chunks} chunks")
    
    # Announce the model metadata first
    announce_model_meta(node, ver, file_size, total_chunks, sha256_hash)
    
    # Send each chunk
    for idx in range(total_chunks):
        start = idx * raw_chunk_size
        end = min(start + raw_chunk_size, file_size)
        chunk_data = data[start:end]
        
        if addr:
            # Send to specific peer
            node.send_model_chunk(ver, idx, total_chunks, chunk_data, addr)
        else:
            # Broadcast to all peers
            node.broadcast_model_chunk(ver, idx, total_chunks, chunk_data)
        
        if idx % 10 == 0 or idx == total_chunks - 1:
            print(f"[DELTA] Sent chunk {idx + 1}/{total_chunks} for ver={ver}")
    
    print(f"[DELTA] Completed sending {total_chunks} chunks for ver={ver}")
    return total_chunks


def handle_incoming_chunk(node, msg: str, addr: Tuple[str, int]):
    """
    Handle an incoming MODEL_CHUNK message.
    This is called by the overlay's handle_message when it receives a MODEL_CHUNK.
    
    Args:
        node: PeerNode instance
        msg: The complete message string
        addr: Source address
    
    Returns:
        True if a complete model was reassembled, False otherwise
    """
    # The chunk handling is already done in udp_overlay.py's handle_message
    # This function checks if we have a complete model and processes it
    
    # Parse the message to get the version
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
        
        ver = fields.get("ver")
        if not ver:
            return False
        
        # Check if this version is complete
        if node.is_model_complete(ver):
            print(f"[DELTA] Model ver={ver} is complete, ready for processing")
            return True
        
        return False
        
    except Exception as e:
        print(f"[ERROR] Failed to process incoming chunk: {e}")
        return False


def receive_and_reassemble(node, ver: str, save_path: str = None) -> str:
    """
    Reassemble a complete model delta from received chunks.
    
    Args:
        node: PeerNode instance
        ver: Version string of the model to reassemble
        save_path: Optional path to save the reassembled file
    
    Returns:
        Path to the reassembled file, or None if incomplete/failed
    """
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
    """
    Get a list of model versions that are currently being received.
    
    Args:
        node: PeerNode instance
    
    Returns:
        List of (version, progress, total) tuples
    """
    pending = []
    for ver, buf in node._model_buffers.items():
        progress = len(buf["parts"])
        total = buf["total"]
        if progress < total:
            pending.append((ver, progress, total))
    return pending


def get_complete_models(node) -> list:
    """
    Get a list of model versions that are complete and ready for processing.
    
    Args:
        node: PeerNode instance
    
    Returns:
        List of version strings
    """
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