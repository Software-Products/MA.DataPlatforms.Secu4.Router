// <copyright file="PacketRoutingService.cs" company="McLaren Applied Ltd.">
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

using MA.DataPlatform.Secu4.RouterComponent.Abstractions;
using MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatform.Secu4.Routing.Contracts;
using MA.DataPlatforms.Abstractions.Logging;
using MA.DataPlatforms.Core;
using MA.DataPlatforms.ServiceConnectors.Abstractions.Services;
using MA.DataPlatforms.ServiceConnectors.Contracts;

using ConsoleLogger = MA.Common.ConsoleLogger;

namespace MA.DataPlatform.Secu4.RouterComponent.UsageSample;

[ExcludeFromCodeCoverage]
public class PacketRoutingService : Service, ISubscriber<ReceivedObjectContract<RoutingDataPacket>>
{
    private readonly IRouter router;

    private int counter;

    public PacketRoutingService(string name, ILogger logger)
        : base(name, logger)
    {
        var routerLogger = new ConsoleLogger();
        var routingConfigurationProvider = new RoutingConfigurationProvider();
        this.router = new Router(
            routerLogger,
            new KafkaProducerBuilder(
                routerLogger,
                routingConfigurationProvider));
        this.router.Initiate();
    }

    public void Do(ReceivedObjectContract<RoutingDataPacket> receivedData)
    {
        this.counter++;
        this.router.Route(receivedData.Data);
        if (this.counter % 1_000_000 == 0)
        {
            this.Logger.Info($"{this.counter} messages received to publish");
        }
    }

    protected override void SetupAfterAllServiceInitiated()
    {
        var listener = this.ServiceHost?.AllServices.OfType<ISubscriptionService<ReceivedObjectContract<RoutingDataPacket>>>()
            .FirstOrDefault(i => i.Name == "KafkaBroking");
        listener?.RegisterSubscriber(this);
        base.SetupAfterAllServiceInitiated();
    }
}
