#!/usr/bin/env python2.7

import argparse
import errno
import urllib2
import sys

import http
import cli

# arg parsing
def parse_args(default_url):
  parser = argparse.ArgumentParser(description="Interact with Rainer-style configuration APIs.")
  decorate_argparser(parser, default_url)
  return parser.parse_args()

def decorate_argparser(parser, default_url):
  if default_url:
    parser.add_argument('--url', metavar='url', default=default_url)
  else:
    parser.add_argument('--url', metavar='url', required=True)

  subparser = parser.add_subparsers(title='commands', dest='mode')
  subparsers = {}

  subparsers["commit"]   = subparser.add_parser('commit')
  subparsers["uncommit"] = subparser.add_parser('uncommit')
  subparsers["edit"]     = subparser.add_parser('edit')
  subparsers["prepare"]  = subparser.add_parser('prepare')
  subparsers["show"]     = subparser.add_parser('show')
  subparsers["log"]      = subparser.add_parser('log')

  for sp in subparsers.values():
    sp.add_argument('key', metavar='key', nargs=1, help='commit key')
    sp.add_argument('version', metavar='version', type=int, nargs="?", help='commit version')

  subparsers["list"] = subparser.add_parser('list')
  subparsers["list"].add_argument('-A', '--all', action='store_true')

def make_client(args):
  return http.RainerClient(args.url)

def make_cli(args):
  return cli.RainerCommandLine(args.url)

# primary logic
def run(default_url=None):
  args = parse_args(default_url)

  if args.mode == "show":
    make_cli(args).action_show(args.key[0], args.version)

  elif args.mode == "log":
    make_cli(args).action_log(args.key[0], args.version)

  elif args.mode == "prepare":
    make_cli(args).action_prepare(args.key[0], args.version)

  elif args.mode == "list":
    make_cli(args).action_list(args.all)

  elif args.mode == "edit":
    make_cli(args).action_edit(args.key[0], args.version)

  elif args.mode == "commit":
    make_cli(args).action_commit(args.key[0])

  elif args.mode == "uncommit":
    make_cli(args).action_uncommit(args.key[0], args.version)

# actual script (logic + signal handling and exits)
def main(default_url=None):
  try:
    run(default_url)
    sys.exit(0)

  except urllib2.HTTPError, e:
    print e
    print e.read()

  except urllib2.URLError, e:
    print 'ERROR:', e.reason

  except KeyboardInterrupt, e:
    pass # Ignore SIGINT

  except IOError, e:
    if e.errno not in [errno.EPIPE]: # Ignore SIGPIPE
      raise e

  sys.exit(1)

if  __name__ == "__main__":
  main()
