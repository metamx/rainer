import argparse
import difflib
import errno
import os
import http
import re
import subprocess
import sys
import tempfile
import termcolor
import termios
import urllib2
import yaml

from commit import RainerCommit

class RainerCommandLine:

  def __init__(self, client_or_uri):
    if isinstance(client_or_uri, http.RainerClient):
      self.client = client_or_uri
    else:
      self.client = http.RainerClient(client_or_uri)

  def action_show(self, key, version=None):
    commit = self.client.get_commit(key, version)
    if commit.value is not None:
      print commit.value,

  def action_prepare(self, key, version=None):
    commit = self.client.get_commit(key, version)
    next_commit = self.next_commit(commit)
    print self.prepare_commit(next_commit),

  def action_list(self, all=False):
    data = self.client.list(all)
    for key in sorted(data.keys()):
      version = data[key]['version']
      print "%s\t%s\t%s" % (key, str(version), self.client.commit_uri(key, version))

  def action_commit(self, key):
    print self.post_prepared_commit(key, sys.stdin.read())

  def action_log(self, key, version=None):
    """CLI log action."""
    old_stdout = sys.stdout
    old = termios.tcgetattr(sys.stdin)
    try:
      # page output
      pager = subprocess.Popen(['less', '-FRSX'], stdin=subprocess.PIPE, stdout=sys.stdout)
      sys.stdout = pager.stdin

      q = self.client.get_commit(key, version)
      while int(q.meta["version"]) > 0:
        if int(q.meta["version"]) == 1:
          p = RainerCommit({"version": 0, "key": q.meta["key"]}, "")
        else:
          p = self.client.get_commit(key, int(q.meta["version"]) - 1)

        print termcolor.colored("commit {:d}".format(q.meta["version"]), "yellow")
        print "Author: {:s}".format(q.meta["author"])
        print "Date:   {:s}".format(q.meta["mtime"])
        print
        print "  {:s}".format(q.meta.get("message", "(no message)"))
        print
        self.__printdiff(
          p.value if p.value is not None else "",
          q.value if q.value is not None else "",
          "{:s} {:d}".format(p.meta["key"], p.meta["version"]),
          "{:s} {:d}".format(q.meta["key"], q.meta["version"]),
        )

        if int(q.meta["version"]) > 0:
          print
          print

        sys.stdout.flush()

        q = p

      pager.stdin.close()
      pager.wait()

    finally:
      sys.stdout = old_stdout
      termios.tcsetattr(sys.stdin, termios.TCSADRAIN, old)

  def action_uncommit(self, key, version=None):
    """CLI uncommit action."""
    commit = self.client.get_commit(key, version)
    next_commit = self.next_commit(commit)
    next_commit.meta["empty"] = True
    next_commit.value = ""
    self.interactive_confirm_and_post(key, commit, self.prepare_commit(next_commit))

  def action_edit(self, key, version=None):
    """CLI edit action."""
    try:
      commit = self.client.get_commit(key, version)
    except urllib2.HTTPError, e:
      if e.code == 404:
        # Commit not found. Make some stuff up.
        commit = RainerCommit({"key": key, "version": 0}, "")
      else:
        raise

    with tempfile.NamedTemporaryFile(delete=False, suffix=".yaml") as tmp:
      editor         = os.environ.get("EDITOR", "vi")
      prepared       = self.prepare_commit(self.next_commit(commit))
      tmp.write(prepared)

    try:
      # open EDITOR on the temp file
      ecode  = subprocess.call([editor + " " + re.escape(tmp.name)], shell=True)
      if ecode != 0:
        raise Exception("Editor %s exited with code %d" % (editor, ecode))

      # read what the editor wrote
      with open(tmp.name, 'r') as tmp:
        newprepared = tmp.read()

      if self.interactive_confirm_and_post(key, commit, newprepared):
        # all successful - remove tmp file
        os.unlink(tmp.name)
      else:
        print "Your work is saved in: " + tmp.name

    except:
      print "An error was encountered. Your work is saved in: " + tmp.name
      raise

  def interactive_confirm_and_post(self, key, old_commit, new_prepared):
    """Display a diff, ask for confirmation on the terminal, and then post (maybe). Returns True if a post occurred."""
    written = False
    if old_commit.meta["key"] != key:
      raise ValueError("Expected key '%s', got '%s'" % (key, old_commit.meta["key"]))

    # extract value for comparison and diffing
    new_prepared_split = self.__splitprepared(new_prepared)

    if old_commit.value != new_prepared_split[1]:
      # show diff
      self.__printdiff(
        old_commit.value if old_commit.value is not None else "",
        new_prepared_split[1],
        "{:s} (current)".format(key),
        "{:s} (new)".format(key)
      )

      # ask for confirmation
      usersaid = raw_input("Commit to %s (yes/no)? " % self.client.commit_uri(key))

      while usersaid != "yes" and usersaid != "no":
        usersaid = raw_input("Please type 'yes' or 'no': ")

      if usersaid == "yes":
        print self.post_prepared_commit(key, new_prepared)
        written = True
      else:
        print "OK, not commiting."

    else:
      print "No change, not commiting."

    return written

  def next_commit(self, commit):
    """Return a commit like this one, but representing the next commit in the series."""
    return RainerCommit(
      {"version" : int(commit.meta["version"]) + 1,
       "author"  : os.getlogin(),
       "comment" : ""},
      commit.value if commit.value is not None else ""
    )

  def prepare_commit(self, commit):
    """Prepare a commit for editing in an editor (convert metadata + value into text form). Returns a string."""
    header = yaml.dump(commit.meta, default_flow_style=False)
    header += "---\n"
    if commit.value is None:
      return bytes(header)
    else:
      return bytes(header) + bytes(commit.value)

  def post_prepared_commit(self, key, prepared):
    """Post a prepared commit back to the API."""
    docs           = self.__splitprepared(prepared)
    docs[0]["key"] = key
    return self.client.post_commit(docs[0], docs[1])

  def __splitprepared(self, prepared):
    # We can't use yaml.load_all since we want to pass the second document straight through. It might not even be
    # a valid yaml document (gasp!)
    docs = prepared.split("\n---\n")
    docs[0] = yaml.load(docs[0])
    return docs

  def __printdiff(self, oldtext, newtext, oldname, newname):
    oldlines = [line + "\n" for line in oldtext.split("\n")]
    newlines = [line + "\n" for line in newtext.split("\n")]

    for line in difflib.unified_diff(oldlines, newlines, str(oldname), str(newname)):
      if line.startswith("---") or line.startswith("+++"):
        sys.stdout.write(termcolor.colored(bytes(line), "white", attrs=["bold"]))
      elif line.startswith("-"):
        sys.stdout.write(termcolor.colored(bytes(line), "red"))
      elif line.startswith("+"):
        sys.stdout.write(termcolor.colored(bytes(line), "green"))
      else:
        sys.stdout.write(bytes(line))
