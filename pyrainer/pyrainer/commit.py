from util import bytes

class RainerCommit:

  def __init__(self, meta, value):
    self.meta = dict(meta)
    self.value = value
    if self.meta.get('empty', False) and value is not None:
      raise ValueError("A value was provided, so 'empty' should be false. But it's not.")
    elif self.meta.get('empty', False) != True and value is None:
      raise ValueError("A value was not provided, so 'empty' should be true. But it's not.")

  @property
  def key(self):
    return self.meta['key']

  @property
  def version(self):
    return self.meta['version']

  @property
  def author(self):
    return self.meta['author']

  @property
  def comment(self):
    return self.meta['comment']

  @property
  def mtime(self):
    return self.meta['mtime']

  @property
  def empty(self):
    return self.meta.get('empty', False)
