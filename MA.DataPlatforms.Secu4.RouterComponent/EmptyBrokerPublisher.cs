// <copyright file="EmptyBrokerPublisher.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking;
using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.RouterComponent;

public class EmptyBrokerPublisher : IBrokerPublisher
{
    private readonly ILogger logger;

    public EmptyBrokerPublisher(ILogger logger)
    {
        this.logger = logger;
    }

    public void Initiate()
    {
        this.logger.Debug("the empty broker publisher initiated");
    }

    public void Publish(RoutingDataPacket data)
    {
        this.logger.Debug("the empty broker published data");
    }

    public void Shutdown()
    {
        this.logger.Debug("the empty broker shutdown");
    }
}
