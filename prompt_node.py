#!/usr/bin/env python3
"""
prompt_node.py — distributed Tiny-GPT2 overlay node
---------------------------------------------------
Each node:
  • Listens for UDP overlay messages (PeerNode)
  • Queries or fine-tunes a tiny GPT-2 locally
  • Exports model deltas and broadcasts to peers
  • Merges incoming deltas in the background

Dependencies:
    pip install torch transformers
"""

import time, threading, torch
from transformers import AutoTokenizer, AutoModelForCausalLM
from udp_overlay import PeerNode, BROADCAST_IP, PORT
from delta_sync import export_delta, broadcast_delta, apply_incoming_deltas


MODEL_NAME = "sshleifer/tiny-gpt2"
LR = 1e-6
DEVICE = "cpu"


# ---------- model utilities ----------
def load_model():
    tok = AutoTokenizer.from_pretrained(MODEL_NAME)
    model = AutoModelForCausalLM.from_pretrained(MODEL_NAME).to(DEVICE)
    torch.save(model.state_dict(), "base.pt")  # ensure baseline
    model.eval()
    return model, tok


def train_on_prompt(model, tok, text, steps=5):
    """Tiny gradient-descent loop for few-shot local fine-tuning."""
    model.train()
    last = None
    for _ in range(steps):
        inputs = tok(text, return_tensors="pt").to(DEVICE)
        outputs = model(**inputs, labels=inputs["input_ids"])
        loss = outputs.loss
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), 1.0)
        for p in model.parameters():
            if p.grad is not None:
                p.data -= LR * p.grad
        model.zero_grad()
        last = float(loss.detach())
    model.eval()
    return last if last is not None else 10.0

def generate_answer(model, tok, question):
    """
    Generate a short, clean, non-repetitive answer.
    Fixes pad_token assignment issue and removes repeated words.
    """
    prompt = f"Q: {question}\nA:"

    # --- ensure tokenizer has a valid pad token ---
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token or "<|endoftext|>"

    # --- encode input ---
    inputs = tok(prompt, return_tensors="pt").to(DEVICE)
    model.eval()

    # --- generate response ---
    with torch.no_grad():
        output_ids = model.generate(
            **inputs,
            max_new_tokens=6,          # limit output length
            do_sample=True,
            temperature=0.7,
            top_p=0.9,
            repetition_penalty=1.3,     # discourage repeated words
            no_repeat_ngram_size=3,     # prevent "factors factors" loops
            num_beams=1,
            pad_token_id=tok.pad_token_id,
            eos_token_id=tok.eos_token_id or tok.pad_token_id,
        )

    # --- decode and clean output ---
    text = tok.decode(output_ids[0], skip_special_tokens=True)

    # extract only what comes after "A:"
    if "A:" in text:
        ans = text.split("A:", 1)[-1]
    else:
        ans = text
    ans = ans.strip()

    # collapse repeated tokens like "factors factors factors"
    words = ans.split()
    cleaned = []
    for w in words:
        if not cleaned or w.lower() != cleaned[-1].lower():
            cleaned.append(w)
    ans = " ".join(cleaned)

    # trim overly long responses
    if len(ans.split()) > 20:
        ans = " ".join(ans.split()[:20]) + " ..."

    return ans

# ---------- background merge thread ----------
def background_merge(node, model):
    while node.running:
        merged = apply_incoming_deltas(node, model)
        if merged:
            print(f"[SYNC] merged {merged} update(s) at {time.strftime('%H:%M:%S')}")
        time.sleep(5)


# ---------- main interactive loop ----------
def main(node_id, debug=False):
    node = PeerNode(node_id)
    node.start()
    model, tok = load_model()

    threading.Thread(target=background_merge, args=(node, model), daemon=True).start()

    try:
        while True:
            mode = input("\nMode: [t]rain / [q]uery / [x]exit: ").strip().lower()
            if mode == "x":
                break

            # ----- local training -----
            elif mode == "t":
                q = input("Question: ").strip()
                a = input("Answer: ").strip()
                if not q or not a:
                    continue
                pair = f"Q: {q}\nA: {a}\n"
                loss = train_on_prompt(model, tok, pair, steps=5)
                print(f"[TRAIN] loss={loss:.4f}")
                path, sha, size = export_delta(model)
                broadcast_delta(node, path, sha, size)

            # ----- querying -----
            elif mode == "q":
                question = input("Ask your question: ").strip()
                if not question:
                    continue
                ans = generate_answer(model, tok, question)
                print(f"[MODEL] {ans}")

                correct = input("Is this correct? (y/n): ").strip().lower()
                if correct == "n":
                    truth = input("Enter correct answer: ").strip()
                    if truth:
                        text = f"Q: {question}\nA: {truth}\n"
                        loss = train_on_prompt(model, tok, text, steps=5)
                        print(f"[UPDATE] loss={loss:.4f}")
                        path, sha, size = export_delta(model)
                        broadcast_delta(node, path, sha, size)

    except KeyboardInterrupt:
        pass
    finally:
        node.stop()
        print("[EXIT] Node stopped.")


# ---------- entry point ----------
if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python3 prompt_node.py <node_id> [--debug]")
        exit(1)
    node_id = sys.argv[1]
    debug = "--debug" in sys.argv
    main(node_id, debug)

