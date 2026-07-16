// <copyright file="MicrosoftConfigurationSecretResolverShould.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Configuration.Core;

using Microsoft.Extensions.Configuration;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class MicrosoftConfigurationSecretResolverShould
{
    private static MicrosoftConfigurationSecretResolver CreateResolver(params (string Key, string Value)[] values)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();
        return new MicrosoftConfigurationSecretResolver(configuration);
    }

    [Fact]
    public void Return_Null_When_Value_Is_Null()
    {
        var resolver = CreateResolver();
        resolver.Resolve(null).Should().BeNull();
    }

    [Fact]
    public void Return_Value_Unchanged_When_Not_A_Placeholder()
    {
        var resolver = CreateResolver();
        resolver.Resolve("literal").Should().Be("literal");
    }

    [Fact]
    public void Resolve_Placeholder_From_Configuration_Key()
    {
        var resolver = CreateResolver(("KAFKA_PASSWORD", "from-vault"));
        resolver.Resolve("${KAFKA_PASSWORD}").Should().Be("from-vault");
    }

    [Fact]
    public void Return_Null_When_Configuration_Key_Is_Missing()
    {
        var resolver = CreateResolver();
        resolver.Resolve("${UNKNOWN_KEY}").Should().BeNull();
    }
}
