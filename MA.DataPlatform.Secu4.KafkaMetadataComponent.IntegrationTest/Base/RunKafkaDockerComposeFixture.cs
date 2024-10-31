// <copyright file="RunKafkaDockerComposeFixture.cs" company="McLaren Applied Ltd.">
//
// Copyright 2024 McLaren Applied Ltd
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// </copyright>

//********************************************************************************************
// Important Note:                                                                           *
// In the pipeline the test will run on ubuntu and docker-compose is run                     *
// by the pipeline so there is no need to run the docker-compose                             *
// This fixture will be used for Windows and running tests on the developer's machine        *
// Just make sure that the visual studio is running with admin privilege                     *
//********************************************************************************************

using Docker.DotNet;
using Docker.DotNet.Models;

using MA.DataPlatforms.Secu4.KafkaMetadataComponent.IntegrationTest.Helper;

[assembly: CollectionBehavior(CollectionBehavior.CollectionPerAssembly, DisableTestParallelization = true)]

namespace MA.DataPlatforms.Secu4.KafkaMetadataComponent.IntegrationTest.Base;

public class RunKafkaDockerComposeFixture : IAsyncLifetime
{
    public async Task DisposeAsync()
    {
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            await Task.Delay(10000);
            return;
        }

        ShellCommandExecutor.Execute("docker-compose", $"-p kafka_metadata down");
        ShellCommandExecutor.Execute("docker", " volume prune -f");

        //give time to docker engine to remove volumes and containers completely
        await Task.Delay(TimeSpan.FromSeconds(10));
    }

    public async Task InitializeAsync()
    {
        if (Environment.OSVersion.Platform != PlatformID.Win32NT)
        {
            await Task.Delay(15000);
            return;
        }

        if (!DockerIsRunning())
        {
            throw new InvalidOperationException("to run the test first need to run the docker engine");
        }

        var testDirectory = AppDomain.CurrentDomain.BaseDirectory;
        ShellCommandExecutor.Execute("net", "stop winnat");
        ShellCommandExecutor.Execute("net", "start winnat");
        ShellCommandExecutor.RunDockerCompose($"{testDirectory}\\docker-compose.yml", "kafka_metadata");
        await WaitForContainerToStart("kafka_meta_data_integration_test_kafka_server");

        //give time to kafka server to fully initialized
        await Task.Delay(TimeSpan.FromSeconds(20));
    }

    private static async Task WaitForContainerToStart(string containerName)
    {
        const int MaxRetries = 60;
        const int RetryIntervalMilliseconds = 1000;
        for (var i = 0; i < MaxRetries; i++)
        {
            Thread.Sleep(RetryIntervalMilliseconds);
            try
            {
                using var client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
                var containers = await client.Containers.ListContainersAsync(
                    new ContainersListParameters
                    {
                        All = true
                    });

                var container = containers.FirstOrDefault(c => c.Names[0].Contains(containerName));
                if (container != null)
                {
                    if (container.State == "running")
                    {
                        Console.WriteLine($"Container '{containerName}' is running.");
                        var hasStarted = await IsContainerStartedAsync(client, container.ID);
                        if (hasStarted)
                        {
                            Console.WriteLine($"Container '{containerName}' has started.");
                            return;
                        }

                        Console.WriteLine($"Container '{containerName}' has not started yet.");
                    }
                    else
                    {
                        Console.WriteLine($"Container '{containerName}' is not running.");
                    }
                }
                else
                {
                    Console.WriteLine($"Container '{containerName}' does not exist.");
                }
            }
            catch (Exception)
            {
                // Retry
            }
        }

        throw new TimeoutException(
            "wait time for running the docker compose elapsed. so please check the docker compose and registry access to see everything is right?");
    }

    private static bool DockerIsRunning()
    {
        try
        {
            using var client = new DockerClientConfiguration(new Uri("npipe://./pipe/docker_engine")).CreateClient();
            var info = client.System.GetSystemInfoAsync().Result;
            return info != null &&
                   !string.IsNullOrEmpty(info.ServerVersion);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<bool> IsContainerStartedAsync(IDockerClient client, string containerId)
    {
        var containerInspect = await client.Containers.InspectContainerAsync(containerId);
        return containerInspect.State.Status == "running";
    }
}
