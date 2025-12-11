// <copyright file="KafkaReaderFactoryShould.cs" company="Motion Applied Ltd.">
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

using FluentAssertions;

using MA.Common.Abstractions;
using MA.DataPlatforms.Secu4.Routing.Shared.Abstractions;

using NSubstitute;

namespace MA.DataPlatforms.Secu4.RouteReaderComponent.UnitTests
{
    public class KafkaReaderFactoryShould
    {
        [Fact]
        public void ReturnKafkaListener_On_Creation_Method_Call()
        {
            //arrange
            var configurationProvider = Substitute.For<IConsumingConfigurationProvider>();
            var cancellationTokenProvider = Substitute.For<ICancellationTokenSourceProvider>();
            var logger = Substitute.For<ILogger>();
            var factory = new KafkaReaderFactory(configurationProvider, cancellationTokenProvider, logger);

            //act
            var listener = factory.Create();

            //assert
            listener.Should().BeOfType<KafkaReader>();
        }
    }
}
