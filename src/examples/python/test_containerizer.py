#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import subprocess
import sys
import os

def launch(container, arguments):
  try:
    data = sys.stdin.read()
    if len(data) <= 0:
      print >> sys.stderr, "Expected protobuf over stdin. Received 0 bytes."
      return 1

    if arguments[0:1] == ["--mesos-executor"]:
        mesos_executor = arguments[1]
    else:
        mesos_executor = "/usr/local/lib/mesos-executor"

    task = mesos_pb2.TaskInfo()
    task.ParseFromString(data)

    if task.HasField("executor"):
        command = ["sh", "-c", task.executor.command.value]
    else:
        print >> sys.stderr, "No executor passed; using Mesos executor!"
        command = [mesos_executor, "sh", "-c", task.command.value]

    proc = subprocess.Popen(command, env=os.environ.copy())
    proc.wait()
  except google.protobuf.message.DecodeError:
    print >> sys.stderr, "Could not deserialise external container protobuf"
    return 1

  return 0

def update(container, arguments):
  data = sys.stdin.readlines()
  # TODO(nnielsen): First, strip first 8 bytes as they contain integer count
  # for number of Resource protos to expect.
  return 0

def usage(container, arguments):
  return 0

def destroy(container, arguments):
  return 0

def recover(container, arguments):
  return 0

def wait(container, arguments):
  return 0

if __name__ == "__main__":
    if sys.argv[1:2] == ["--help"] or sys.argv[1:2] == ["-h"]:
      print "Usage: %s <command> <container-id>" % sys.argv[0]
      sys.exit(0)

    if len(sys.argv) < 3:
        print >> sys.stderr, "Please pass a method and a container ID"
        sys.exit(1)

    command = sys.argv[1]
    method = { "launch":  launch,
               "update":  update,
               "destroy": destroy,
               "usage":   usage,
               "wait":    wait }.get(command)

    if method is None:
      print >> sys.stderr, "No valid method selected"
      sys.exit(2)

    import mesos
    import mesos_pb2
    import google

    sys.exit(method(sys.argv[2], sys.argv[3:]))