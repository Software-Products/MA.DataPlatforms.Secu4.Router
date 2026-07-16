// <copyright file="KafkaConfigOverrideWhitelist.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.Routing.Configuration.FileConfig;

/// <summary>
/// Optional allow/deny list controlling which broker-config-file properties are permitted to
/// override the running Kafka producer/consumer configuration.
///
/// <para>
/// When no whitelist file is supplied, every property present in the broker config file is applied
/// (current behaviour). When a whitelist is supplied, each candidate property — discovered by
/// reflection over the broker config file — is matched against the ordered rule list using a
/// <b>first-match-wins</b> ("first pass, first pick") strategy. The first rule whose
/// <see cref="KafkaConfigOverrideRule.Pattern"/> matches the property name decides whether the
/// override is applied (<see cref="KafkaConfigOverrideRule.Allow"/>). A property that matches no
/// rule is denied (whitelist semantics).
/// </para>
///
/// <para>
/// Patterns are wildcard-based and matched (case-insensitive) against the Confluent.Kafka property
/// names (e.g. <c>compression.type</c>, <c>linger.ms</c>, <c>batch.size</c>). <c>*</c> matches any
/// sequence of characters and <c>?</c> matches a single character.
/// </para>
/// </summary>
public class KafkaConfigOverrideWhitelist
{
    /// <summary>Ordered rules applied to the publisher broker config file.</summary>
    public List<KafkaConfigOverrideRule> Publish { get; set; } = [];

    /// <summary>Ordered rules applied to the consumer broker config file.</summary>
    public List<KafkaConfigOverrideRule> Consume { get; set; } = [];
}

/// <summary>
/// A single ordered allow/deny rule. The first rule (in list order) whose <see cref="Pattern"/>
/// matches the candidate property name determines whether that property may override the running
/// configuration.
/// </summary>
public class KafkaConfigOverrideRule
{
    /// <summary>
    /// Wildcard pattern matched against the Confluent.Kafka property name (case-insensitive).
    /// <c>*</c> matches any sequence of characters; <c>?</c> matches a single character.
    /// Examples: <c>compression.*</c>, <c>linger.ms</c>, <c>*</c>.
    /// </summary>
    public string Pattern { get; set; } = "*";

    /// <summary>
    /// <see langword="true"/> to allow the matched property to override the running config;
    /// <see langword="false"/> to keep the existing (baseline) value. Defaults to <see langword="true"/>.
    /// </summary>
    public bool Allow { get; set; } = true;
}
