// <copyright file="EmptyBrokerPublisherShould.cs" company="McLaren Applied Ltd.">
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

using MA.Common.Abstractions;
using MA.DataPlatform.Secu4.Routing.Contracts;

using NSubstitute;

using Xunit;

namespace MA.DataPlatform.Secu4.RouterComponent.UnitTest.Core;

public class EmptyBrokerPublisherShould
{
    private readonly ILogger logger;

    private readonly EmptyBrokerPublisher emptyBrokerPublisher;

    public EmptyBrokerPublisherShould()
    {
        this.logger = Substitute.For<ILogger>();
        this.emptyBrokerPublisher = new EmptyBrokerPublisher(this.logger);
    }

    [Fact]
    public void Log_Upon_Initiate()
    {
        //arrange done in ctor

        //act
        this.emptyBrokerPublisher.Initiate();

        //assert
        this.logger.Received(1).Debug("the empty broker publisher initiated");
    }

    [Fact]
    public void Log_Upon_Publish()
    {
        //arrange done in ctor

        //act
        this.emptyBrokerPublisher.Publish(new RoutingDataPacket(Array.Empty<byte>(), ""));

        //assert
        this.logger.Received(1).Debug("the empty broker published data");
    }

    [Fact]
    public void Log_Upon_ShutDown()
    {
        //arrange done in ctor

        //act
        this.emptyBrokerPublisher.Shutdown();

        //assert
        this.logger.Received(1).Debug("the empty broker shutdown");
    }
}