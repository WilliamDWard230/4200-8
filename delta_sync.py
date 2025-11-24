
import os, io, torch, hashlib, tempfile, time
from udp_overlay import PeerNode, BROADCAST_IP, PORT
from update_exchanges import announce_model_meta, fragment_and_send, handle_incoming_chunk, receive_and_reassemble


def export_delta(model, threshold=1e-6):
    # compare model to baseline
    base = torch.load("base.pt", map_location="cpu")
    now = model.state_dict()
    delta = {}

    # only keep tensors whose diff passes threshold
    for k, v in now.items():
        diff = (v - base[k])
        if torch.norm(diff) > threshold:
            delta[k] = diff.half()
    
    # save delta to a temp file
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pt")
    torch.save(delta, tmp.name)
    # compute sha and size
    sha = hashlib.sha256(open(tmp.name, "rb").read()).hexdigest()
    size = os.path.getsize(tmp.name)
    return tmp.name, sha, size



def broadcast_delta(node, path, sha, size):

    # validating delta file
    if not os.path.exists(path):
        print(f"[DELTA] File not found, cannot broadcast: {path}")
        return None

    # warnings for mismatches
    actual_size = os.path.getsize(path)
    if actual_size != size:
        print(f"[DELTA] Warning: size mismatch for {path}: expected={size}, actual={actual_size}")

    try:
        with open(path, "rb") as f:
            data = f.read()
        actual_sha = hashlib.sha256(data).hexdigest()
        if sha and sha != actual_sha:
            print(f"[DELTA] Warning: SHA mismatch for {path}: export={sha[:8]}..., recomputed={actual_sha[:8]}...")
    except Exception as e:
        print(f"[DELTA] Warning: could not verify SHA for {path}: {e}")

    # create version id
    ver = f"{int(time.time())}-{sha[:8]}" if sha else str(int(time.time()))

    # send file in chunks
    print(f"[DELTA] Broadcasting delta ver={ver} from {path}")
    chunks = fragment_and_send(node, ver, path, addr=None)
    print(f"[DELTA] Broadcast of ver={ver} complete ({chunks} chunks sent)")
    return ver


def reassemble_delta(node: PeerNode, ver: str):

    # check if all chunks are present
    if not node.is_model_complete(ver):
        print(f"[DELTA] Model ver={ver} is not complete yet")
        return None

    # pull data from chunks for model reassembly
    data = node.get_reassembled_model(ver)
    if data is None:
        print(f"[DELTA] Failed to reassemble data for ver={ver}")
        return None

    # fetch size and hash
    buf = node._model_buffers.get(ver, {})
    expected_sha = buf.get("sha256", "")
    expected_size = buf.get("size", 0)

    # verify size and hash
    if expected_size and len(data) != expected_size:
        print(f"[DELTA] Warning: size mismatch for ver={ver}: expected={expected_size}, actual={len(data)}")

    if expected_sha:
        actual_sha = hashlib.sha256(data).hexdigest()

        if actual_sha != expected_sha:
            print(f"[DELTA] Hash mismatch for ver={ver}!")
            print(f"  expected: {expected_sha}")
            print(f"  actual:   {actual_sha}")
            return None
        else:
            print(f"[DELTA] Hash verified for ver={ver}")
    else:
        print(f"[DELTA] No expected SHA stored for ver={ver}, skipping hash verification")
    
    # return raw data
    return data




def apply_incoming_deltas(node, model, merge_weight=1.0):
    merged = 0

    # for each buffered version
    for ver, buf in list(node._model_buffers.items()):
        last_idx  = buf.get("_last_idx", 0)
        last_total = buf["total"]

        # reassemble the version
        data = reassemble_delta(node, ver)
        if data is None:
            continue

        # load delta and add into matching layers
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".pt")
        tmp.write(data)
        tmp.close()

        try:
            delta = torch.load(tmp.name, map_location="cpu", weights_only=False)
            sd = model.state_dict()
            applied = 0

            for j, v in delta.items():
                if j in sd and sd[j].shape == v.shape:
                    sd[j] = sd[j] + (merge_weight * v.to(sd[j].dtype))
                    applied += 1

            # save updated model
            model.load_state_dict(sd)
            torch.save(model.state_dict(), "base.pt")
            print(f"[MERGE] model {ver} applied ✓ ({applied} layers)")
            merged += 1

        except Exception as e:
            print(f"[ERROR] failed to merge {ver}: {e}")

        # clean temp files and buffers
        finally:
            os.remove(tmp.name)
            node._model_buffers.pop(ver, None)

    return merged

