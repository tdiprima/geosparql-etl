import hashlib
import os
from pathlib import Path

import requests
from dotenv import load_dotenv
from urllib3.exceptions import InsecureRequestWarning

load_dotenv()

# Suppress InsecureRequestWarning when using verify=False
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

DRUPAL_BASE = "https://quip.bmi.stonybrook.edu/node"


def get_auth():
    """Get authentication credentials from environment variables."""
    username = os.getenv("DRUPAL_USER")
    password = os.getenv("DRUPAL_PASS")

    if not username or not password:
        raise ValueError(
            "Missing credentials: Set DRUPAL_USER and DRUPAL_PASS environment variables"
        )

    return (username, password)


def get_drupal_json(node_id: int, auth=None) -> dict:
    """
    Grab JSON from Drupal for a given path DB ID.

    Args:
        node_id: The Drupal node ID
        auth: Optional authentication tuple (username, password) or other auth handler
    """
    url = f"{DRUPAL_BASE}/{node_id}?_format=json"
    resp = requests.get(url, verify=False, auth=auth)
    resp.raise_for_status()
    return resp.json()


def extract_file_path(drupal_json: dict) -> str:
    """
    Extract the file path from the Drupal JSON.
    The file path is stored in field_iip_path[0]['value'].
    """
    return drupal_json["field_iip_path"][0]["value"]


def compute_sha256(path: str) -> str:
    """Compute SHA256 from file bytes."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def get_real_hash_from_node(node_id: int, auth=None) -> str:
    """Full pipeline: node → JSON → file path → sha256."""
    j = get_drupal_json(node_id, auth=auth)
    file_path = extract_file_path(j)

    p = Path(file_path)
    if not p.exists():
        raise FileNotFoundError(f"File not found at {p}, fix path mapping")

    return compute_sha256(str(p))


if __name__ == "__main__":
    # Get credentials from environment variables
    auth = get_auth()
    try:
        result = get_real_hash_from_node(7, auth=auth)
        print(f"SHA256 hash for node 7: {result}")
    except FileNotFoundError as e:
        print(f"Error: {e}")
        print("Please check your path mapping configuration or update the file path in Drupal.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
