// <copyright file="DataGeneratorService.cs" company="McLaren Applied Ltd.">
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

using System.Diagnostics.CodeAnalysis;

using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatforms.Abstractions.Logging;
using MA.DataPlatforms.Core;
using MA.DataPlatforms.ServiceConnectors.Abstractions.Services;

namespace MA.DataPlatform.Secu4.RouterComponent.UsageSample;

[ExcludeFromCodeCoverage]
public class DataGeneratorService : Service
{
    private static readonly Random Random = new();
    private IPublisherService<RoutingDataPacket>? publisher;

    public DataGeneratorService(string name, ILogger logger)
        : base(name, logger)
    {
    }

    protected override void SetupAfterAllServiceInitiated()
    {
        this.publisher = this.ServiceHost?.AllServices.OfType<IPublisherService<RoutingDataPacket>>().FirstOrDefault(i => i.Name == "KafkaBroking");
        base.SetupAfterAllServiceInitiated();
    }

    protected override void Starting()
    {
        Task.Run(
            () =>
            {
                this.Logger.Info("wait for 5 seconds  all services started first and then publishing data...");
                Task.Delay(5000).Wait();
                this.Logger.Info("start publishing data...");
                for (var i = 0; i < 10_000_000; i++)
                {
                    var dataBytes = CreateDataBytes();
                    this.publisher?.Publish(new RoutingDataPacket(dataBytes, "test"));
                    if (i % 2000 == 0)
                    {
                        Task.Delay(1).Wait();
                    }

                    if ((i + 1) % 1_000_000 == 0)
                    {
                        this.Logger.Info($"{(i + 1)} messages published");
                    }
                }
            });
    }

    private static byte[] CreateDataBytes()
    {
        var dtBytes = BitConverter.GetBytes(DateTime.Now.Ticks);
        var randombytes = GetRandomByteArray(138 - 8);
        var bytes = new byte[138];
        Array.Copy(dtBytes, 0, bytes, 0, 8);
        Array.Copy(randombytes, 0, bytes, 8, randombytes.Length);
        return bytes;
    }

    private static byte[] GetRandomByteArray(long size)
    {
        var data = new byte[size];
        Random.NextBytes(data);
        return data;
    }
}
