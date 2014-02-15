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
#include <iostream>
#include <list>
#include <set>
#include <sstream>

#include <errno.h>
#include <signal.h>
#include <stdio.h>
#include <poll.h>

#include <process/defer.hpp>
#include <process/delay.hpp>
#include <process/id.hpp>
#include <process/io.hpp>

#include <stout/check.hpp>
#include <stout/exit.hpp>
#include <stout/foreach.hpp>
#include <stout/lambda.hpp>
#include <stout/nothing.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>
#include <stout/uuid.hpp>

#include "common/type_utils.hpp"

#include "slave/containerizer/pluggable_containerizer.hpp"
#include "slave/paths.hpp"

using std::list;
using std::map;
using std::string;
using std::vector;
using std::set;
using std::stringstream;

using namespace process;

namespace mesos {
namespace internal {
namespace slave {

using state::SlaveState;
using state::FrameworkState;
using state::ExecutorState;
using state::RunState;


PluggableContainerizer::PluggableContainerizer(const Flags& flags)
{
  process = new PluggableContainerizerProcess(flags);
  spawn(process);
}


PluggableContainerizer::~PluggableContainerizer()
{
  terminate(process);
  process::wait(process);
  delete process;
}


Future<Nothing> PluggableContainerizer::recover(const Option<state::SlaveState>& state)
{
  return dispatch(process, &PluggableContainerizerProcess::recover, state);
}


Future<Nothing> PluggableContainerizer::launch(
    const ContainerID& containerId,
    const TaskInfo& taskInfo,
    const ExecutorInfo& executorInfo,
    const string& directory,
    const Option<string>& user,
    const SlaveID& slaveId,
    const PID<Slave>& slavePid,
    bool checkpoint)
{
  return dispatch(process,
                  &PluggableContainerizerProcess::launch,
                  containerId,
                  taskInfo,
                  executorInfo,
                  directory,
                  user,
                  slaveId,
                  slavePid,
                  checkpoint);
}


Future<Nothing> PluggableContainerizer::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  return dispatch(process,
                  &PluggableContainerizerProcess::update,
                  containerId,
                  resources);
}


Future<ResourceStatistics> PluggableContainerizer::usage(
    const ContainerID& containerId)
{
  return dispatch(process, &PluggableContainerizerProcess::usage, containerId);
}


Future<Containerizer::Termination> PluggableContainerizer::wait(
    const ContainerID& containerId)
{
  return dispatch(process, &PluggableContainerizerProcess::wait, containerId);
}


void PluggableContainerizer::destroy(const ContainerID& containerId)
{
  dispatch(process, &PluggableContainerizerProcess::destroy, containerId);
}




PluggableContainerizerProcess::PluggableContainerizerProcess(
    const Flags& _flags) :
    flags(_flags)
{
}


Future<Nothing> PluggableContainerizerProcess::recover(
    const Option<state::SlaveState>& state)
{
  LOG(INFO) << "Recovering containerizer";

  // Gather the executor run states that we will attempt to recover.
  list<RunState> recoverable;
  if (state.isSome()) {
    foreachvalue (const FrameworkState& framework, state.get().frameworks) {
      foreachvalue (const ExecutorState& executor, framework.executors) {
        LOG(INFO) << "Recovering executor '" << executor.id
                  << "' of framework " << framework.id;

        if (executor.info.isNone()) {
          LOG(WARNING) << "Skipping recovery of executor '" << executor.id
                       << "' of framework " << framework.id
                       << " because its info could not be recovered";
          continue;
        }

        if (executor.latest.isNone()) {
          LOG(WARNING) << "Skipping recovery of executor '" << executor.id
                       << "' of framework " << framework.id
                       << " because its latest run could not be recovered";
          continue;
        }

        // We are only interested in the latest run of the executor!
        const ContainerID& containerId = executor.latest.get();
        CHECK(executor.runs.contains(containerId));
        const RunState& run = executor.runs.get(containerId).get();

        if (run.completed) {
          VLOG(1) << "Skipping recovery of executor '" << executor.id
                  << "' of framework " << framework.id
                  << " because its latest run '" << containerId << "'"
                  << " is completed";
          continue;
        }

        recoverable.push_back(run);
      }
    }
  }

  // TODO(nnielsen): recoverable needs further action.

  return Nothing();
}


// TODO(*): Use new API call which pass a full TaskInfo.
Future<Nothing> PluggableContainerizerProcess::launch(
      const ContainerID& containerId,
      const TaskInfo& taskInfo,
      const ExecutorInfo& executorInfo,
      const string& directory,
      const Option<string>& user,
      const SlaveID& slaveId,
      const process::PID<Slave>& slavePid,
      bool checkpoint)
{
  LOG(INFO) << "Launch triggered for containerId '" << containerId << "'";

  if (running.contains(containerId)) {
    LOG(ERROR) << "Cannot start already running container '"
               << containerId << "'";
    return Failure("Containerizer already started");
  }

  const map<string, string>& env = executorEnvironment(
      executorInfo,
      directory,
      slaveId,
      slavePid,
      checkpoint,
      flags.recovery_timeout);

  foreachpair (const string& key, const string& value, env) {
    os::setenv(key, value);
  }

/*
  CommandInfo& command = taskInfo.has_executor()
    ? *(taskInfo.mutable_executor()->mutable_command())
    : *(taskInfo.mutable_command());

  if (!command.has_container()) {
    if (flags.default_container.isSome()) {
      command.mutable_container()->set_image(flags.default_container.get());
    } else {
      LOG(INFO) << "No container specified in task and no default given. "
                << "Containerizer will have to fill in defaults.";
    }
  }
*/
  // Send our containerizer protobuf via pipe.
  stringstream output;
  LOG(INFO) << taskInfo.DebugString();
  taskInfo.SerializeToOstream(&output);

  vector<string> parameters;
  parameters.push_back("--mesos-executor");
  parameters.push_back(path::join(flags.launcher_dir, "mesos-executor"));

  Try<pid_t> invoked = invoke(
      "launch",
      containerId,
      output.str(),
      parameters);

  if (invoked.isError()) {
    // We generaly need to use an internal implementation in these cases.
    // For the specific case of a launch however, there can not be an
    // internal implementation for a pluggable containerizer, hence we
    // need to fail or even abort at this point.
    LOG(ERROR) << "launch: '" << invoked.error() << "' "
               << "was returned for containerId "
               << "'" << containerId << "' on pluggable containerizer";
    return Failure(invoked.error());

    // TODO(tillt): Find out why even though we clearly signalled that we
    // were not able to launch, we still get called again for a wait on this
    // containerId.
  }

  LOG(INFO) << "Launch has forked for containerId '" << containerId << "' "
            << "at pid " << invoked.get();

  // Observe the process status and install a callback for status changes.
  process::reap(invoked.get())
    .onAny(defer(
      PID<PluggableContainerizerProcess>(this),
      &PluggableContainerizerProcess::reaped,
      containerId,
      lambda::_1));

  // Record the process.
  running.put(containerId, Owned<Running>(new Running(invoked.get())));

  LOG(INFO) << "launch: Finishing up for: " << containerId;

  return Nothing();
}


void PluggableContainerizerProcess::reaped(
    const ContainerID& containerId,
    const Future<Option<int> >& status)
{
  CHECK(running.contains(containerId));

  Timer::cancel(running[containerId]->terminationTimeout);

  LOG(INFO) << "containerId '" << containerId
            << "' has terminated with status: "
            << (status.isReady() ? "READY" :
               status.isFailed() ? "FAILED: " + status.failure() :
               "DISCARDED");

  Future<Containerizer::Termination> future;
  if (!status.isReady()) {
    // Something has gone wrong, probably an unsuccessful terminate().
    future = Failure(
        "Failed to get status: " +
        (status.isFailed() ? status.failure() : "discarded"));
  } else {
    future = Containerizer::Termination(
        status.get(), false, "Containerizer terminated");
  }

  // Set the promise to alert others waiting on this container.
  running[containerId]->termination.set(future);

  // Ensure someone notices this termination by deferring final clean up until
  // the container has been waited on.
  running[containerId]->waited.future()
    .onAny(defer(PID<PluggableContainerizerProcess>(this),
                 &PluggableContainerizerProcess::cleanup,
                 containerId));
}


void PluggableContainerizerProcess::cleanup(
    const ContainerID& containerId)
{
  LOG(INFO) << "Callback performing final cleanup of running state";

  if (!running.contains(containerId)) {
    LOG(WARNING) << "containerId '" << containerId << "' not running anymore";
    return;
  }

  running.erase(containerId);
}


Future<Containerizer::Termination> PluggableContainerizerProcess::wait(
    const ContainerID& containerId)
{
  LOG(INFO) << "Wait triggered on containerId '" << containerId << "'";
  CHECK(running.contains(containerId));

  string input;
  Try<pid_t> invoked = invokeAwaitingResult("wait", containerId, input);
  if (invoked.isError()) {
    LOG(ERROR) << "wait: '" << invoked.error() << "' "
               << "was returned for containerId "
               << "'" << containerId << "' on pluggable containerizer";
    return Failure(invoked.error());
  }

  Try<bool> result = checkInvocation(input, waitForExitCode(invoked.get()));
  if (result.isSome()) {
    if (!result.get()) {
      LOG(ERROR) << "wait: for containerId '" << containerId << "' a "
                 << "non-default implementation was indicated but "
                 << "containerizers must use the default!";
      return Failure("No default!");
    }
    Owned<Running> run = running[containerId];
    // Final clean up is delayed until someone has waited on the
    // container so set the future to indicate this has occurred.
    if (!run->waited.future().isReady()) {
      LOG(INFO) << "containerId: " << containerId << " is not ready yet";
      run->waited.set(true);
    }
    return run->termination.future();
  } else {
    return Failure(result.error());
  }
}



Future<Nothing> PluggableContainerizerProcess::update(
    const ContainerID& containerId,
    const Resources& resources)
{
  LOG(INFO) << "Update triggered on containerId '" << containerId << "'";

  // TODO(tillt): It appears that we get update requests on
  // containerIds that have not (yet) been launched. This appears
  // suspicious but at this stage we kind of ignore this fact and
  // return.
  if (!running.contains(containerId)) {
    LOG(WARNING) << "containerId '" << containerId << "' not running";
    return Nothing();
  }
  running[containerId]->resources = resources;

  // Wrap the Resource protobufs into a ResourceArray protobuf to
  // avoid any problems with streamed protobufs.
  // See http://goo.gl/d1x14F for more on that issue.
  ResourceArray resourceArray;
  foreach (const Resource& r, resources) {
    Resource *resource = resourceArray.add_resource();
    resource->CopyFrom(r);
  }

  // Render our ResourceArray protobuf to the output stream.
  stringstream output;
  resourceArray.SerializeToOstream(&output);

  string input;
  Try<pid_t> invoked = invokeAwaitingResult(
      "update", containerId, output.str(), input);

  if (invoked.isError()) {
    LOG(ERROR) << "update: '" << invoked.error() << "' "
               << "was returned for containerId "
               << "'" << containerId << "' on pluggable containerizer";
    return Failure(invoked.error());
  }

  Try<bool> result = checkInvocation(input, waitForExitCode(invoked.get()));
  if (result.isError()) {
    return Failure(result.error());
  }
  LOG(INFO) << "update: Ignoring updates";
  return Nothing();
}


Future<ResourceStatistics> PluggableContainerizerProcess::usage(
    const ContainerID& containerId)
{
  LOG(INFO) << "Usage triggered on containerId '" << containerId << "'";

  CHECK(running.contains(containerId));

  ResourceStatistics result;

  string input;
  Try<pid_t> invoked = invokeAwaitingResult("usage", containerId, input);

  if (invoked.isError()) {
    LOG(ERROR) << "usage: '" << invoked.error() << "' "
               << "was returned for containerId "
               << "'" << containerId << "' on pluggable containerizer";
    return Failure(invoked.error());
  }

  Try<bool> checked = checkInvocation(input, waitForExitCode(invoked.get()));
  if (checked.isError()) {
    return Failure(checked.error());
  }

  if (checked.get()) {
    LOG(INFO) << "using defaults for 'usage'";

    // Pluggable does not support this command, invoke fallback implementation.
    pid_t pid = running[containerId]->pid;

    LOG(INFO) << "Getting usage for process PID " << pid << " "
              << "within containerId '" << containerId << "' "
              << "via fallback implementation";

    Result<os::Process> process = os::process(pid);

    if (!process.isSome()) {
      return Failure(
          process.isError() ?
              process.error() :
              "Process does not exist or may have terminated already");
    }

    result.set_timestamp(Clock::now().secs());

    // Set the resource allocations.
    const Resources& resources = running[containerId]->resources;
    const Option<Bytes>& mem = resources.mem();
    if (mem.isSome()) {
      result.set_mem_limit_bytes(mem.get().bytes());
    }

    const Option<double>& cpus = resources.cpus();
    if (cpus.isSome()) {
      result.set_cpus_limit(cpus.get());
    }

    if (process.get().rss.isSome()) {
      result.set_mem_rss_bytes(process.get().rss.get().bytes());
    }

    // We only show utime and stime when both are available, otherwise
    // we're exposing a partial view of the CPU times.
    if (process.get().utime.isSome() && process.get().stime.isSome()) {
      result.set_cpus_user_time_secs(process.get().utime.get().secs());
      result.set_cpus_system_time_secs(process.get().stime.get().secs());
    }

    // Now aggregate all descendant process usage statistics.
    const Try<set<pid_t> >& children = os::children(pid, true);

    if (children.isError()) {
      return Failure(
          "Failed to get children of " + stringify(pid) + ": " +
          children.error());
    }

    // Aggregate the usage of all child processes.
    foreach (pid_t child, children.get()) {
      process = os::process(child);

      // Skip processes that disappear.
      if (process.isNone()) {
        continue;
      }

      if (process.isError()) {
        LOG(WARNING) << "Failed to get status of descendant process " << child
                     << " of parent " << pid << ": "
                     << process.error();
        continue;
      }

      if (process.get().rss.isSome()) {
        result.set_mem_rss_bytes(
            result.mem_rss_bytes() + process.get().rss.get().bytes());
      }

      // We only show utime and stime when both are available, otherwise
      // we're exposing a partial view of the CPU times.
      if (process.get().utime.isSome() && process.get().stime.isSome()) {
        result.set_cpus_user_time_secs(
            result.cpus_user_time_secs() + process.get().utime.get().secs());
        result.set_cpus_system_time_secs(
            result.cpus_system_time_secs() + process.get().stime.get().secs());
      }
    }
  } else {
    LOG(INFO) << "'usage' supported";
    // Pluggable should have delivered a protobuf, attempt to parse it.
    bool parsed = result.ParseFromString(input);

    if (!parsed) {
      // A parse failure in connection with an exit(0) means that the command
      // is supported but it delivered invalid data.
      // TODO(tillt): We need to kill the external containerizer process.
      EXIT(1) << "Failed to parse protobuf describing usage with error: "
              << result.DebugString();
    }
  }

  LOG(INFO) << "total mem usage "
            << result.mem_rss_bytes() << " "
            << "total CPU user usage "
            << result.cpus_user_time_secs() << " "
            << "total CPU system usage "
            << result.cpus_system_time_secs();

  return result;
}


void PluggableContainerizerProcess::destroyTimeout(
    const ContainerID& containerId)
{
  LOG(INFO) << "Elevated kill on containerId '" << containerId << "'";

  if (!running.contains(containerId)) {
    LOG(INFO) << "containerId '" << containerId << "' got reaped already";
    return;
  }

  LOG(WARNING) << "containerId '" << containerId << "' was signalled a "
               << "SIGTERM but that did not cause termination within 5 "
               << "seconds - sending a SIGKILL now";

  // TODO(tillt): Are we sure that the process did not get reaped after the
  // last line and before this one?
  pid_t pid = running[containerId]->pid;

  // Kill the containerizer and all processes in the containerizer's process
  // group and session.
  Try<list<os::ProcessTree> > trees =
    os::killtree(pid, SIGKILL, true, true);

  if (trees.isError()) {
    LOG(WARNING) << "Failed to kill the process tree rooted at pid "
                 << pid << ": " << trees.error();

    // Manually call reaped with a failed status to set a failed
    // Termination which will be seen by the slave.
    reaped(containerId, Failure("Failed to kill process group"));
  } else {
    LOG(WARNING) << "Killed the following process trees:\n"
                 << stringify(trees.get());
  }
}


void PluggableContainerizerProcess::destroy(const ContainerID& containerId)
{
  LOG(INFO) << "Destroy triggered on containerId '" << containerId << "'";

  CHECK(running.contains(containerId));

  Try<pid_t> invoked = invoke("destroy", containerId);
  if (invoked.isSome()) {
    waitForExitCode(invoked.get());
  }

  // Ignore result for now - we need to terminate the process anyways.

  pid_t pid = running[containerId]->pid;

  // Kill the containerizer and all processes in the containerizer's process
  // group and session.
  Try<list<os::ProcessTree> > trees =
    os::killtree(pid, SIGTERM, true, true);

  if (trees.isError()) {
    LOG(WARNING) << "Failed to terminate the process tree rooted at pid "
                 << pid << ": " << trees.error();
  } else {
    LOG(INFO) << "Terminated the following process trees:\n"
              << stringify(trees.get());
  }

  // TODO(tillt): No/Error by killtree does not in any way signal if a process
  // got properly terminated.
  // We would need to introduce a delay and possibly send a SIGTERM to be
  // sure about that. But then we just do not know how much time the
  // containerizer takes for shutting down gracefully.
  // For now, I shall go ahead and add such delay but I feel bad about it.
  running[containerId]->terminationTimeout = delay(
      Seconds(5),
      PID<PluggableContainerizerProcess>(this),
      &PluggableContainerizerProcess::destroyTimeout,
      containerId);
}


int PluggableContainerizerProcess::waitForExitCode(const pid_t& pid)
{
  int exitCode = 1;

  LOG(INFO) << "Awaiting process termination of pid: " << pid;

  // Wait for external containerizer command invoke to terminate.
  int status;
  while(waitpid(pid, &status, 0) < 0);

  if(WIFEXITED(status)) {
    exitCode = WEXITSTATUS(status);
  }
  return exitCode;
}


Try<bool> PluggableContainerizerProcess::checkInvocation(
  const string& input,
  const int exitCode)
{
  if (exitCode != 0) {
    return Error("External containerizer failed utterly (exit: " +
                 stringify(exitCode) + ")");
  }
  bool unimplemented = input.length() == 0;
  if (unimplemented) {
    LOG(INFO) << "External containerizer exited 0 and had no output, which "
              << "requests the default implementation";
  }
  return unimplemented;
}


Try<pid_t> PluggableContainerizerProcess::invoke(
    const string& command,
    const ContainerID& containerId)
{
  string input;
  return invoke(command, containerId, input);
}


Try<pid_t> PluggableContainerizerProcess::invoke(
    const string& command,
    const ContainerID& containerId,
    const string& input)
{
  vector<string> parameters;
  return invoke(command, containerId, input, parameters);
}


Try<pid_t> PluggableContainerizerProcess::invoke(
    const string& command,
    const ContainerID& containerId,
    const string& input,
    const vector<string>& parameters)
{
  int pipe;

  Try<pid_t> invoked = callContainerizer(
      command,
      parameters,
      containerId,
      input,
      pipe);
  if (invoked.isSome()) {
    os::close(pipe);
  }
  return invoked;
}


Try<pid_t> PluggableContainerizerProcess::invokeAwaitingResult(
    const string& command,
    const ContainerID& containerId,
    string& output)
{
  string input;
  return invokeAwaitingResult(command, containerId, input, output);
}


Try<pid_t> PluggableContainerizerProcess::invokeAwaitingResult(
    const string& command,
    const ContainerID& containerId,
    const string& input,
    string& output)
{
  LOG(INFO) << "Calling out for method '" << command << "'";

  int pipe;
  vector<string> parameters;
  Try<pid_t> invoked = callContainerizer(
      command,
      parameters,
      containerId,
      input,
      pipe);

  if (invoked.isSome()) {
    Try<Nothing> nonblock = os::nonblock(pipe);

    if (nonblock.isError()) {
      os::close(pipe);
      return Error("Failed to accept, nonblock: " + nonblock.error());
    }
    // Read all data from the input pipe until it is closed be the sender.
    Future<string> read = io::read(pipe);

    // Block until done.
    read.await();

    os::close(pipe);

    if (read.isReady()) {
      output = read.get();
    } else if (read.isFailed()) {
      return Error(read.failure());
    }
  }

  return invoked;
}

Try<pid_t> PluggableContainerizerProcess::callContainerizer(
    const string& command,
    const vector<string>& parameters,
    const ContainerID& containerId,
    const string& input,
    int& outputPipe)
{
  CHECK(flags.containerizer_path.isSome())
    << "Pluggable containerizer path not set";

  LOG(INFO) << "Calling out for method '" << command << "'";

  stringstream containerIdStream;
  containerIdStream << containerId;

  // Construct the argument vector.
  vector<string> argv;
  argv.push_back(flags.containerizer_path.get());
  argv.push_back(command);
  argv.push_back(containerIdStream.str());
  if (parameters.size()) {
    argv.insert(argv.end(), parameters.begin(), parameters.end());
  }

  return callExternal(input, argv, outputPipe);
}

Try<pid_t> PluggableContainerizerProcess::callExternal(
    const string& input,
    const vector<string>& argv,
    int& outputPipe)
{
  LOG(INFO) << "callExternal: [" << strings::join(" ", argv) << "]";

  vector<const char*> cstrings;
  foreach (const string& arg, argv) {
    cstrings.push_back(arg.c_str());
  }
  cstrings.push_back(NULL);

  // This cast is needed to match the signature of execvp().
  char* const* plainArgumentArray = (char* const*) &cstrings[0];

  int childToParentPipe[2];
  int parentToChildPipe[2];
  if ((pipe(childToParentPipe) < 0) || (pipe(parentToChildPipe) < 0)) {
    return Error(string("Failed to create pipes: ") + strerror(errno));
  }

  // Fork exec of the external process.
  pid_t pid;
  if ((pid = fork()) == -1) {
    // TODO(tillt): We may need to kill the external containerizer process.
    perror("Failed to fork new containerizer");
    abort();
  }

  if (pid > 0) {
    // In parent process context.
    os::close(childToParentPipe[1]);
    os::close(parentToChildPipe[0]);

    // Get child pid from pipe as it may have changed due to a
    // setsid failure, triggering an additional fork.
    if (::read(childToParentPipe[0], &pid, sizeof(pid)) != sizeof(pid)) {
      // TODO(tillt): We may need to kill the external containerizer process.
      perror("Failed to read process id from pipe");
      abort();
    }

    if (input.length() > 0) {
      LOG(INFO) << "Writing to child's standard input "
                << "(" << input.length() << " bytes)";

      ssize_t len = input.length();
      if (write(parentToChildPipe[1], input.c_str(), len) < len) {
        perror("Failed to write protobuf to pipe");
        abort();
      }
    }

    // Close write pipe as we are done sending.
    os::close(parentToChildPipe[1]);

    // Return child's standard output.
    outputPipe = childToParentPipe[0];
  } else {
    // In child process context.
    os::close(childToParentPipe[0]);
    os::close(parentToChildPipe[1]);

    // We need to be rather careful at this point not to use async-
    // unsafe code in between fork and exec. This is why there is e.g.
    // no glog allowed in between fork and exec.

    // Put containerizer into its own process session to prevent its
    // termination to be propagated all the way to its parent process
    // (the slave).

    // NOTE: We setsid() in a loop because setsid() might fail if another
    // process has the same process group id as the calling process.
    pid_t ppid;
    while ((ppid = setsid()) == -1) {
      perror("Could not put executor in its own session");
      if ((ppid = fork()) == -1) {
        perror("Failed to fork pluggable containerizer");
        abort();
      }
      if (ppid > 0) {
        // In parent process.
        exit(0);
      }
    }

    // Send the child's pid via pipe as it may have changed.
    if (write(childToParentPipe[1], &ppid, sizeof(ppid)) != sizeof(ppid)) {
      perror("Failed to write child PID to pipe");
      abort();
    }

    // Replace stdin and stdout.
    dup2(parentToChildPipe[0], fileno(stdin));
    dup2(childToParentPipe[1], fileno(stdout));

    // Close pipes.
    os::close(childToParentPipe[1]);
    os::close(parentToChildPipe[0]);

    // Execute the containerizer command.
    execvp(plainArgumentArray[0], plainArgumentArray);

    // If we get here, the exec call failed.
    perror("Failed to execute the pluggable containerizer:");
    abort();
  }

  return pid;
}


} // namespace slave {
} // namespace internal {
} // namespace mesos {
