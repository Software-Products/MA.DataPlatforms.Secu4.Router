// <copyright file="KafkaBrokerPublisher.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatform.Secu4.RouterComponent.Abstractions.Broking;
using MA.DataPlatform.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatform.Secu4.Routing.Contracts;

namespace MA.DataPlatform.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;

public class KafkaBrokerPublisher : IBrokerPublisher
{
    private readonly ILogger logger;
    private readonly IKafkaProducerHolder producerHolder;

    public KafkaBrokerPublisher(ILogger logger, IKafkaProducerHolder kafkaProducerHolder)
    {
        this.logger = logger;
        this.producerHolder = kafkaProducerHolder;
    }

    public void Initiate()
    {
        this.logger.Debug("Initiating the producer");
        this.producerHolder.Initiate();
        this.logger.Debug("initiation of the producer complete");
    }

    public void Publish(RoutingDataPacket data)
    {
        this.logger.Debug("publishing data");
        this.producerHolder.Publish(data);
        this.logger.Debug("publishing data completed");
    }

    public void Shutdown()
    {
        this.logger.Debug("shutting down the producer");
        this.producerHolder.Dispose();
        this.logger.Debug("the producer shutdown complete");
    }
}
