// <copyright file="KafkaConfigOverrideWhitelistMatcherShould.cs" company="Motion Applied Ltd.">
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
using MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

using Xunit;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.UnitTest.Core;

public class KafkaConfigOverrideWhitelistMatcherShould
{
    private static List<KafkaConfigOverrideRule> Rules(params (string pattern, bool allow)[] rules) =>
        rules.Select(r => new KafkaConfigOverrideRule { Pattern = r.pattern, Allow = r.allow }).ToList();

    [Fact]
    public void Allow_Everything_When_No_Rules()
    {
        KafkaConfigOverrideWhitelistMatcher.IsAllowed(null, "LingerMs").Should().BeTrue();
        KafkaConfigOverrideWhitelistMatcher.IsAllowed([], "LingerMs").Should().BeTrue();
    }

    [Fact]
    public void Deny_When_No_Rule_Matches()
    {
        var rules = Rules(("Compression*", true));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "LingerMs").Should().BeFalse();
    }

    [Fact]
    public void Allow_When_A_Rule_Matches()
    {
        var rules = Rules(("LingerMs", true));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "LingerMs").Should().BeTrue();
    }

    [Fact]
    public void Use_First_Matching_Rule_Wins()
    {
        // Deny the specific property before the broad allow.
        var rules = Rules(("CompressionLevel", false), ("Compression*", true));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "CompressionLevel").Should().BeFalse();
        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "CompressionType").Should().BeTrue();
    }

    [Fact]
    public void Match_Any_Of_The_Candidate_Aliases()
    {
        // Pattern written as the dotted librdkafka key still matches when the C# name is also supplied.
        var rules = Rules(("linger.ms", true), ("*", false));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "LingerMs", "linger.ms").Should().BeTrue();
    }

    [Fact]
    public void Match_Case_Insensitively()
    {
        var rules = Rules(("lingerms", true));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "LingerMs").Should().BeTrue();
    }

    [Theory]
    [InlineData("*", true)]
    [InlineData("Linger?s", true)]
    [InlineData("Linger?", false)]
    public void Support_Wildcards(string pattern, bool expected)
    {
        var rules = Rules((pattern, true));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "LingerMs").Should().Be(expected);
    }

    [Fact]
    public void Catch_All_Deny_Blocks_Unlisted_Properties()
    {
        var rules = Rules(("BatchSize", true), ("*", false));

        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "BatchSize").Should().BeTrue();
        KafkaConfigOverrideWhitelistMatcher.IsAllowed(rules, "Acks").Should().BeFalse();
    }
}
