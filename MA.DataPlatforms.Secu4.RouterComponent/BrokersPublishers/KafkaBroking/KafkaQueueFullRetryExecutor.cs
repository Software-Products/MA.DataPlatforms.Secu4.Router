// <copyright file="KafkaQueueFullRetryExecutor.cs" company="McLaren Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;

namespace MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;

public class KafkaQueueFullRetryExecutor : IKafkaQueueFullRetryExecutor
{
    private readonly IKafkaProducer producer;
    private readonly ILogger logger;

    public KafkaQueueFullRetryExecutor(ILogger routerLogger, IKafkaProducer producer)
    {
        this.producer = producer;
        this.logger = routerLogger;
    }

    public void Execute(byte[] message, string topic, int? partition = null, string? key = null)
    {
        this.logger.Error($"{DateTime.Now.TimeOfDay}:local queue full wait for 500ms for retry");
        Task.Delay(TimeSpan.FromMilliseconds(500)).Wait();
        this.producer.Produce(message, topic, partition, key);
    }
}
