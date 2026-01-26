// <copyright file="RouterTester.cs" company="Motion Applied Ltd.">
//
// Copyright 2025 Motion Applied Ltd
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

using MA.Common;
using MA.DataPlatforms.Secu4.RouterComponent;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Contracts;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Core;

namespace MA.DataPlatforms.Secu4.Routing.Profiling;

[ExcludeFromCodeCoverage]
internal class RouterTester
{
    private readonly KafkaRouter kafkaRouter;
    private readonly List<RoutingDataPacket> lstMessages;

    public RouterTester(int numberOfMessages, int sizeOfContent)
    {
        var logger = new ConsoleLogger();
        var routerRepository = new RouteReadingWritingComponentRepository<KafkaRouter>(logger);
        this.kafkaRouter = new KafkaRouter(
            logger,
            new KafkaProducerBuilder(logger, new RoutingConfigurationProvider(), new KafkaRouteManager(logger)),
            routerRepository,
            "TestRouter");
        this.lstMessages = [];
        var rnd = new Random();

        for (var i = 0; i < numberOfMessages; i++)
        {
            var bytes = new byte[sizeOfContent];
            rnd.NextBytes(bytes);
            this.lstMessages.Add(new RoutingDataPacket(bytes, "test", DateTime.UtcNow));
        }

        foreach (var router1 in routerRepository.GetAll())
        {
            router1.ShutDown();
        }
    }

    public void Setup()
    {
        this.kafkaRouter.Initiate();
    }

    public void Start()
    {
        foreach (var t in this.lstMessages)
        {
            this.kafkaRouter.Route(t);
        }

        WaitHandler.WaitEvent.Set();
    }
}
