// <copyright file="TopicInfo.cs" company="Motion Applied Ltd.">
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

namespace MA.DataPlatforms.Secu4.KafkaMetadataComponent;

public class TopicInfo
{
    public TopicInfo(string topicName, int partition, long offset)
    {
        this.TopicName = topicName;
        this.Partition = partition;
        this.Offset = offset;
    }

    public string TopicName { get; }

    public int Partition { get; }

    public long Offset { get; }
}
