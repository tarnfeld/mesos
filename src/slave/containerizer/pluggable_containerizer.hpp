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

#ifndef __PLUGGABLE_CONTAINERIZER_HPP__
#define __PLUGGABLE_CONTAINERIZER_HPP__

#include <sstream>

#include <stout/hashmap.hpp>
#include <stout/lambda.hpp>
#include <stout/try.hpp>

#include <process/timer.hpp>
#include <process/owned.hpp>
#include <process/reap.hpp>

#include "slave/containerizer/containerizer.hpp"
#include "slave/containerizer/isolator.hpp"
#include "slave/containerizer/launcher.hpp"

namespace mesos {
namespace internal {
namespace slave {


// Forward declaration.
class PluggableContainerizerProcess;

class PluggableContainerizer : public Containerizer
{
public:
  PluggableContainerizer(const Flags& flags);

  virtual ~PluggableContainerizer();

  virtual process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  virtual process::Future<Nothing> launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  virtual process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  virtual process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  virtual process::Future<Containerizer::Termination> wait(
      const ContainerID& containerId);

  virtual void destroy(const ContainerID& containerId);

private:
  PluggableContainerizerProcess* process;
};

class PluggableContainerizerProcess : public process::Process<PluggableContainerizerProcess>
{
public:
  PluggableContainerizerProcess(const Flags& flags);

  // Recover containerized executors as specified by state Any containerized
  // executors present on the system but not included in state will be
  // terminated and cleaned up.
  process::Future<Nothing> recover(
      const Option<state::SlaveState>& state);

  // Start the containerized executor as given in executorInfo. In particular,
  // only the resources specified in executorInfo will be available to the
  // executor. This method will return when the executor has exec'ed.
  process::Future<Nothing> launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const std::string& directory,
      const Option<std::string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint);

  // Update the executor's resources. The executor should not assume the
  // resources have been changed until this method returns.
  process::Future<Nothing> update(
      const ContainerID& containerId,
      const Resources& resources);

  // Gather resource usage statistics for the containerized executor.
  process::Future<ResourceStatistics> usage(
      const ContainerID& containerId);

  // Get a future on the containerized executor's Termination.
  process::Future<Containerizer::Termination> wait(const ContainerID& containerId);

  // Terminate the containerized executor.
  void destroy(const ContainerID& containerId);

private:
  // Startup flags.
  const Flags flags;

  // Information describing a running container process.
  struct Running
  {
    Running(pid_t _pid) : pid(_pid) {}

    pid_t pid;
    process::Promise<Containerizer::Termination> termination;
    process::Promise<bool> waited;
    process::Timer terminationTimeout;
    Resources resources;
  };

  // Stores all launched processes.
  hashmap<ContainerID, process::Owned<Running> > running;


  // If the exit code is 0 and the data sent on stdout is empty,
  // it is taken to mean that the external containerizer is
  // requesting Mesos use the default strategy for a particular
  // method (usage, wait, and all the rest). Should the external
  // containerizer exit non-zero, it is always an error.
  Try<bool> checkInvocation(const std::string& input,
      const int exitCode);

  // Test if pipe is ready for reading.
  Result<bool> isReadable(const Result<int>& pipe);

  // Test for input on pipe by reading a single byte from it.
  Result<bool> hasData(const Result<int>& pipe);

  // Get the exit code of a short-lived process.
  int waitForExitCode(const pid_t& pid);

  // Call back for when the pluggable containerizer process status has changed.
  void reaped(const ContainerID& containerId,
      const process::Future<Option<int> >& status);

  // Installed via destroy, sends a SIGKILL to a process group.
  void destroyTimeout(const ContainerID& containerId);

  // Call back for when the containerizer has terminated all processes in the
  // container.
  void cleanup(const ContainerID& containerId);

  Try<pid_t> invoke(
      const std::string& command,
      const ContainerID& containerId);

  Try<pid_t> invoke(
      const std::string& command,
      const ContainerID& containerId,
      const std::string& input);

  Try<pid_t> invoke(
      const std::string& command,
      const ContainerID& containerId,
      const std::string& input,
      const std::vector<std::string>& parameters);

  Try<pid_t> invokeAwaitingResult(
      const std::string& command,
      const ContainerID& containerId,
      std::string& output);

  Try<pid_t> invokeAwaitingResult(
      const std::string& command,
      const ContainerID& containerId,
      const std::string& input,
      std::string& output);

  Try<pid_t> callContainerizer(
      const std::string& command,
      const std::vector<std::string>& parameters,
      const ContainerID& containerId,
      const std::string& input,
      int& outputPipe);

  Try<pid_t> callExternal(
      const std::string& input,
      const std::vector<std::string>& argv,
      int& outputPipe);
};

} // namespace slave {
} // namespace internal {
} // namespace mesos {

#endif // __PLUGGABLE_CONTAINERIZER_HPP__

