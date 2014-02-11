/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>

#include <process/id.hpp>
#include <process/defer.hpp>

#include <stout/check.hpp>

#include <boost/algorithm/string/join.hpp>

#include "slave/flags.hpp"
#include "slave/docker_isolator.hpp"
#include "slave/slave.hpp"

namespace mesos {
namespace internal {
namespace slave {

DockerIsolator::DockerIsolator()
  : ProcessBase(ID::generate("docker-isolator")),
    initialized(false) {}


void DockerIsolator::initialize(
    const Flags& _flags,
    const Resources& _,
    bool _local,
    const PID<Slave>& _slave)
{
  flags = _flags;
  local = _local;
  slave = _slave;

  initialized = true;
}


void DockerIsolator::launchExecutor(
    const SlaveID& slaveId,
    const FrameworkID& frameworkId,
    const FrameworkInfo& frameworkInfo,
    const ExecutorInfo& executorInfo,
    const UUID& uuid,
    const std::string& directory,
    const Resources& resources)
{
  CHECK(initialized) << "Cannot launch executors before initialization!";

  const ExecutorID& executorId = executorInfo.executor_id();
  const IsolationInfo& isolationInfo = executorInfo.isolation_info();

  // Convert the arguments into a list of strings
  std::list<std::string> dockerArguments;
  for (int i = 0; i < isolationInfo.args_size(); i++) {
    dockerArguments.push_back(isolationInfo.args(i));
  }

  ContainerInfo *container = new ContainerInfo(
    frameworkId,
    executorId,
    dockerArguments,
    uuid.toString()
  );

  infos[frameworkId][executorId] = container;

  LOG(INFO) << "Launching docker container " << container->dockerContainerId
            << " with image " << isolationInfo.image()
            << " with arguments '" << boost::algorithm::join(dockerArguments, " ")
            << "' (" << executorInfo.command().value() << ")"
            << " with resources " << resources
            << " for framework " << frameworkId;

  std::list<std::string> args;

  args.push_back("docker");
  args.push_back("-H");
  args.push_back("192.168.4.2:7070");
  args.push_back("run");
  args.push_back("-name");
  args.push_back(container->dockerContainerId);

  // We get the environment map for launching mesos-launcher before
  // the fork, because we have seen deadlock issues with ostringstream
  // in the forked process before it calls exec.
  // map<std::string, std::string> env = launcher.getLauncherEnvironment();
  // foreachpair (const std::string& key, const std::string& value, env) {
  //   args.push_back("-e");
  //   args.push_back(key << "=" << value);
  // }

  args.insert(args.end(), dockerArguments.begin(), dockerArguments.end());

  args.push_back(isolationInfo.image());
  args.push_back(executorInfo.command().value());

  LOG(INFO) << "Docker invoke " << boost::algorithm::join(args, " ");

  int pipes[2];
  if (pipe(pipes) < 0) {
    PLOG(FATAL) << "Failed to create a pipe";
  }

  // Set the FD_CLOEXEC flags on these pipes
  Try<Nothing> cloexec = os::cloexec(pipes[0]);
  CHECK_SOME(cloexec) << "Error setting FD_CLOEXEC on pipe[0]";

  cloexec = os::cloexec(pipes[1]);
  CHECK_SOME(cloexec) << "Error setting FD_CLOEXEC on pipe[1]";

  // Fork
  pid_t pid;
  if ((pid = fork()) == -1) {
    PLOG(FATAL) << "Failed to fork to launch docker container";
  }

  if (pid > 0) {
    os::close(pipes[1]);

    // Get the child's pid via the pipe.
    if (read(pipes[0], &pid, sizeof(pid)) == -1) {
      PLOG(FATAL) << "Failed to get child PID from pipe";
    }

    os::close(pipes[0]);

    // In parent process.
    LOG(INFO) << "Forked executor at " << pid;

    reaper.monitor(pid)
      .onAny(defer(PID<DockerIsolator>(this),
                   &DockerIsolator::reaped,
                   pid,
                   lambda::_1));

    // Tell the slave this executor has started.
    dispatch(slave, &Slave::executorStarted, frameworkId, executorId, pid);
  } else {
    // In child process, we make cleanup easier by putting process
    // into it's own session. DO NOT USE GLOG!
    os::close(pipes[0]);

    char *exec_args[args.size() + 1];
    int count = 0;

    for (std::list<std::string>::iterator arg = args.begin(); arg != args.end(); arg++) {
        exec_args[count] = strdup(arg->c_str());
        count++;
    }

    exec_args[count] = NULL;

    LOG(INFO) << "Launching docker";

    execvp(exec_args[0], exec_args);

    // If we get here, the execvp call failed.
    perror("Failed to execvp the mesos-launcher");
    abort();
  }
}


void DockerIsolator::killExecutor(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  CHECK(initialized) << "Cannot kill executors before initialization!";
  LOG(INFO) << "Killing executor " << executorId;
}


void DockerIsolator::resourcesChanged(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId,
    const Resources& resources)
{
  CHECK(initialized) << "Cannot do resourcesChanged before initialization!";
  LOG(INFO) << "Resources changed for " << executorId;
}


process::Future<ResourceStatistics> DockerIsolator::usage(
    const FrameworkID& frameworkId,
    const ExecutorID& executorId)
{
  ResourceStatistics result;
  return result;
}


process::Future<Nothing> DockerIsolator::recover(
    const Option<state::SlaveState>& state)
{
  return Nothing();
}


void DockerIsolator::reaped(pid_t pid, const process::Future<Option<int> >& status)
{
  // foreachkey (const FrameworkID& frameworkId, infos) {
  //   foreachkey (const ExecutorID& executorId, infos[frameworkId]) {
  //     ContainerInfo* info = infos[frameworkId][executorId];

  //     if (info->pid.isSome() && info->pid.get() == pid) {
  //       if (!status.isReady()) {
  //         LOG(ERROR) << "Failed to get the status for executor '" << executorId
  //                    << "' of framework " << frameworkId << ": "
  //                    << (status.isFailed() ? status.failure() : "discarded");
  //         return;
  //       }

  //       LOG(INFO) << "Telling slave of terminated executor '" << executorId
  //                 << "' of framework " << frameworkId;

  //       dispatch(slave,
  //                &Slave::executorTerminated,
  //                frameworkId,
  //                executorId,
  //                status.get(),
  //                false,
  //                "Executor terminated");

  //       if (!info->killed) {
  //         // Try and cleanup after the executor.
  //         killExecutor(frameworkId, executorId);
  //       }

  //       if (infos[frameworkId].size() == 1) {
  //         infos.erase(frameworkId);
  //       } else {
  //         infos[frameworkId].erase(executorId);
  //       }
  //       delete info;

  //       return;
  //     }
  //   }
  // }
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
