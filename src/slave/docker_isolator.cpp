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

#include <stout/check.hpp>

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

  const IsolationInfo& isolationInfo = executorInfo.isolation_info();

  LOG(INFO) << "ISOLATION " << isolationInfo.image();
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

} // namespace slave {
} // namespace internal {
} // namespace mesos {
