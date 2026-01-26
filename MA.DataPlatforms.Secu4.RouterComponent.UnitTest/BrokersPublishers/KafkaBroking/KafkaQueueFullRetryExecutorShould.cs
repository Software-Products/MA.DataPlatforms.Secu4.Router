// <copyright file="KafkaQueueFullRetryExecutorShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.RouterComponent.Abstractions.Broking.KafkaBroking;
using MA.DataPlatforms.Secu4.RouterComponent.BrokersPublishers.KafkaBroking;

using NSubstitute;

using Xunit;

namespace MA.DataPlatforms.Secu4.RouterComponent.UnitTest.BrokersPublishers.KafkaBroking;

public class KafkaQueueFullRetryExecutorShould
{
    [Fact]
    public void Log_The_Queue_Full_Message_And_Retry_After_500_ms()
    {
        //arrange
        var producer = Substitute.For<IKafkaProducer>();
        var logger = Substitute.For<ILogger>();
        var executor = new KafkaQueueFullRetryExecutor(logger, producer);

        //act
        const string TestTopic = "test_topic";
        var message = new byte[]
        {
            1, 2, 3
        };
        executor.Execute(message, TestTopic);

        //assert
        logger.Received(1).Error(Arg.Is<string>(i => i.EndsWith(":local queue full wait for 500ms for retry", StringComparison.Ordinal)));
        producer.Received(1).Produce(message, TestTopic);
    }
}
