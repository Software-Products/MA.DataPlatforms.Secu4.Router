// <copyright file="RouterShould.cs" company="Motion Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions;
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatforms.Secu4.Routing.Contracts;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.RouterComponent.UnitTest.Core;

public class RouterShould
{
    private readonly ILogger logger = Substitute.For<ILogger>();
    private readonly IRoutingConfigurationProvider configurationProvider = Substitute.For<IRoutingConfigurationProvider>();
    private readonly IKafkaProducerHolderBuilder kafkaProducerBuilder = Substitute.For<IKafkaProducerHolderBuilder>();

    [Fact]
    public void Broker_Publisher_Initiate_Call_After_Initiate()
    {
        //arrange 
        this.configurationProvider.Provide().Returns(new RoutingConfiguration(new KafkaRoutingConfig(new KafkaPublishingConfig(), [], [], "")));
        var router = new Router(this.logger, this.kafkaProducerBuilder);

        //act
        router.Initiate();

        //assert
        this.kafkaProducerBuilder.Received(1).Build();
    }
}
