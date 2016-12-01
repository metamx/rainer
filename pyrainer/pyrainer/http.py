from . import commit
import json
import urllib.request, urllib.error, urllib.parse
import base64

class RainerClient:

  def __init__(self, base_uri):
    if not base_uri:
      raise ValueError("base_uri is required")
    self.base_uri = base_uri

  def commit_uri(self, key, version=None):
    """Base URI for a commit key, possibly at a specific version."""
    return self.base_uri + "/" + key + (("/" + str(version)) if version != None else "")

  def list(self, all=False):
    """Get a dict of commit key -> metadata for the most recent versions."""
    rsp = urllib.request.urlopen(self.base_uri + ("?all=yes" if all else ""))
    return json.loads(rsp.read().decode("utf-8"))

  def list_full(self, all=False):
    """Get a dict of commit key -> RainerCommit object for the most recent versions."""
    rsp = urllib.request.urlopen(self.base_uri + "?payload_base64=yes" + ("&all=yes" if all else ""))
    rsp_json = json.loads(rsp.read().decode("utf-8"))
    for key, value in rsp_json.items():
      rsp_json[key] = commit.RainerCommit(value, base64.b64decode(value['payload_base64']))
    return rsp_json

  def get_commit(self, key, version=None):
    """Get commit object, possibly at a specific version."""
    meta = self.get_metadata(key, version)
    if meta.get("empty", False):
      value = None
    else:
      value = self.get_value(key, meta["version"])
    return commit.RainerCommit(meta, value)

  def get_value(self, key, version=None):
    """Get commit value (a string), possibly at a specific version."""
    rsp = urllib.request.urlopen(self.commit_uri(key, version))
    return rsp.read().decode("utf-8")

  def get_metadata(self, key, version=None):
    """Get commit metadata (a dict), possibly at a specific version."""
    rsp = urllib.request.urlopen(self.commit_uri(key, version) + "/meta")
    return json.loads(rsp.read().decode("utf-8"))

  def post_commit(self, metadata, value):
    """Save a new commit."""
    req = urllib.request.Request(self.commit_uri(metadata["key"], metadata["version"]), bytes(value, "utf-8"), {
      "Content-Type"     : "application/octet-stream",
      "X-Rainer-Author"  : metadata["author"],
      "X-Rainer-Comment" : metadata["comment"],
      "X-Rainer-Empty"   : str(metadata.get("empty", "false"))
    })
    rsp = urllib.request.urlopen(req)
    return json.loads(rsp.read().decode("utf-8"))
