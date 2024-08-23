// <copyright file="KeyGeneratorServiceShould.cs" company="McLaren Applied Ltd.">
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

using System.Diagnostics;

using FluentAssertions;

using MA.Common;
using MA.Common.Abstractions;

using NSubstitute;

using Xunit;
using Xunit.Abstractions;

namespace MA.DataPlatform.Secu4.RouterComponent.UnitTest.Common
{
    public class KeyGeneratorServiceShould
    {
        private readonly ITestOutputHelper outputHelper;
        private readonly ILoggingDirectoryProvider loggingDirectoryProvider = Substitute.For<ILoggingDirectoryProvider>();
        private readonly KeyGeneratorService keyGenerator;

        public KeyGeneratorServiceShould(ITestOutputHelper outputHelper)
        {
            this.outputHelper = outputHelper;
            this.loggingDirectoryProvider.Provide().Returns("");
            this.keyGenerator = new KeyGeneratorService(this.loggingDirectoryProvider);
        }

        [Fact]
        public void Generate_Unique_Ulong_Key()
        {
            //arrange
            const int NumberOfGeneratingKeys = 1_000_000;
            var lstCreated = new List<ulong>();

            //act
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            for (var i = 0; i < NumberOfGeneratingKeys; i++)
            {
                lstCreated.Add(this.keyGenerator.GenerateUlongKey());
            }

            stopWatch.Stop();

            //assert
            lstCreated.GroupBy(i => i).Any(i => i.Count() > 1).Should().Be(false);
            this.outputHelper.WriteLine($"elapsed time to generate {NumberOfGeneratingKeys} keys: {stopWatch.ElapsedMilliseconds} ms");
        }

        [Fact]
        public void Generate_Unique_String_Key()
        {
            //arrange
            const int NumberOfGeneratingKeys = 1_000_000;
            var lstCreated = new List<string>();

            //act
            var stopWatch = new Stopwatch();
            stopWatch.Start();
            for (var i = 0; i < NumberOfGeneratingKeys; i++)
            {
                lstCreated.Add(this.keyGenerator.GenerateStringKey());
            }

            stopWatch.Stop();

            //assert
            lstCreated.GroupBy(i => i).Any(i => i.Count() > 1).Should().Be(false);
            this.outputHelper.WriteLine($"elapsed time to generate {NumberOfGeneratingKeys} keys: {stopWatch.ElapsedMilliseconds} ms");
        }
    }
}
