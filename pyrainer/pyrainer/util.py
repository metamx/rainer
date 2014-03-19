# wtf python
def bytes(s):
  if isinstance(s, str):
    return s
  elif isinstance(s, unicode):
    return s.encode("utf8")
  else:
    return str(s)
