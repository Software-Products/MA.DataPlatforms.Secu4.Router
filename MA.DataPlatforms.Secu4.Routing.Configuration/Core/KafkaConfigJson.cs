// <copyright file="KafkaConfigJson.cs" company="Motion Applied Ltd.">
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

using System.Text.Json;
using System.Text.Json.Serialization;

namespace MA.DataPlatforms.Secu4.Routing.Configuration.Core;

/// <summary>
/// Shared <see cref="JsonSerializerOptions"/> used by every JSON configuration
/// provider so the deserialisation behaviour (case-insensitive property names
/// and string-based enum handling) is defined in a single place.
/// </summary>
internal static class KafkaConfigJson
{
    /// <summary>
    /// Case-insensitive options with a <see cref="JsonStringEnumConverter"/>.
    /// </summary>
    public static JsonSerializerOptions Options { get; } = new()
    {
        PropertyNameCaseInsensitive = true,
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>
    /// Reads and deserialises a JSON config file at <paramref name="path"/>.
    /// </summary>
    /// <typeparam name="T">The configuration type to deserialise into.</typeparam>
    /// <param name="path">Path to the JSON file.</param>
    public static T ReadFile<T>(string path)
    {
        var json = File.ReadAllText(path);
        return JsonSerializer.Deserialize<T>(json, Options)
            ?? throw new InvalidOperationException($"Failed to deserialise '{typeof(T).Name}' config from '{path}'.");
    }

    /// <summary>
    /// Returns the set of top-level property names physically present in the JSON file at
    /// <paramref name="path"/> (case-insensitive). Used by the broker override providers to apply
    /// only the fields the user actually specified, leaving everything else at its existing value
    /// rather than the file model's defaults.
    /// </summary>
    public static HashSet<string> ReadPresentTopLevelKeys(string path)
    {
        var keys = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        if (doc.RootElement.ValueKind == JsonValueKind.Object)
        {
            foreach (var property in doc.RootElement.EnumerateObject())
            {
                keys.Add(property.Name);
            }
        }

        return keys;
    }

    /// <summary>
    /// Reads a flat JSON object of raw librdkafka properties (e.g. <c>{ "linger.ms": 0,
    /// "compression.type": "zstd" }</c>) into a case-insensitive key/value map. Scalar values
    /// (string/number/bool) are converted to their librdkafka string form; nested objects/arrays
    /// and nulls are skipped. Keys starting with <c>_</c> (comment keys) are ignored.
    /// </summary>
    public static Dictionary<string, string> ReadRawProperties(string path)
    {
        var properties = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        using var doc = JsonDocument.Parse(File.ReadAllText(path));
        if (doc.RootElement.ValueKind == JsonValueKind.Null)
            throw new InvalidOperationException($"Failed to read broker properties from '{path}': file content is null.");
        if (doc.RootElement.ValueKind != JsonValueKind.Object)
        {
            return properties;
        }

        foreach (var property in doc.RootElement.EnumerateObject())
        {
            if (property.Name.StartsWith('_'))
            {
                continue;
            }

            switch (property.Value.ValueKind)
            {
                case JsonValueKind.String:
                    properties[property.Name] = property.Value.GetString() ?? string.Empty;
                    break;
                case JsonValueKind.Number:
                    properties[property.Name] = property.Value.GetRawText();
                    break;
                case JsonValueKind.True:
                    properties[property.Name] = "true";
                    break;
                case JsonValueKind.False:
                    properties[property.Name] = "false";
                    break;
            }
        }

        return properties;
    }
}
