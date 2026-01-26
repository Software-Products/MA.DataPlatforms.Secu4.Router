// <copyright file="KafkaTopicMetaDataComparer.cs" company="Motion Applied Ltd.">
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

using MA.DataPlatforms.Secu4.Routing.Contracts;

namespace MA.DataPlatforms.Secu4.Routing.Shared.Core;

public sealed class KafkaTopicMetaDataComparer(StringComparer? topicComparer = null) : IEqualityComparer<KafkaTopicMetaData>
{
    private readonly StringComparer topicComparer = topicComparer ?? StringComparer.OrdinalIgnoreCase;

    public bool Equals(KafkaTopicMetaData? x, KafkaTopicMetaData? y)
    {
        if (ReferenceEquals(x, y))
        {
            return true;
        }

        if (x is null ||
            y is null)
        {
            return false;
        }

        return this.topicComparer.Equals(x.Topic, y.Topic)
               && x.NumberOfPartitions == y.NumberOfPartitions
               && x.ReplicationFactor == y.ReplicationFactor;
    }

    public int GetHashCode(KafkaTopicMetaData? obj)
    {
        if (obj is null)
        {
            return 0;
        }

        unchecked
        {
            var hash = 17;
            hash = hash * 23 + this.topicComparer.GetHashCode(obj.Topic);
            hash = hash * 23 + (obj.NumberOfPartitions?.GetHashCode() ?? 0);
            hash = hash * 23 + (obj.ReplicationFactor?.GetHashCode() ?? 0);
            return hash;
        }
    }
}
