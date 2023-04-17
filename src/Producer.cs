using Aliyun.MQ;
using Aliyun.MQ.Model;
using Hestia.MQ.Abstractions;
using System;
using System.Diagnostics;
using HestiaMessage = Hestia.MQ.Abstractions.Message;

namespace Hestia.RocketMQ
{
    public class Producer : IProducer
    {
        private readonly MQProducer producer;
        private readonly string format = null;
        private readonly string charset = null;

        public Producer(MQProducer producer,string format,string charset)
        {
            this.producer = producer;
            this.format = format ?? Utility.DefaultFormatPrefix;
            this.charset = charset ?? Utility.DefaultCharsetPrefix;
        }

        public string Publish(HestiaMessage message)
        {
            TopicMessage request = BuildPublishMessage(message);
            TopicMessage response = producer.PublishMessage(request);
            Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<Publish>{producer.IntanceId}/{producer.TopicName}/{response.Id}({message.Tag})");
            return response.Id;
        }

        private TopicMessage BuildPublishMessage(HestiaMessage source)
        {
            var target = new TopicMessage(Utility.Transform(source.Body, nameof(HestiaMessage.Body), format,charset,source.Properties,Utility.Encode), source.Tag);

            if (!string.IsNullOrEmpty(source.Key))
            {
                target.MessageKey = source.Key;
            }

            if (source.Properties.TryGetValue(nameof(TopicMessage.ShardingKey), out var key) && !string.IsNullOrEmpty(key))
            {
                target.ShardingKey = key;
            }

            foreach (var property in source.Properties)
            {
                if (string.IsNullOrEmpty(property.Key) || string.IsNullOrEmpty(property.Value)) { continue; }
                if (property.Key.StartsWith(format) || property.Key.StartsWith(charset)) { target.Properties.Add(property.Key, property.Value); }

                var value = Utility.PublishMessagePropertyMapper.ContainsKey(property.Key) ? Utility.PublishMessagePropertyMapper[property.Key].Invoke(property.Value) : Utility.Transform(property.Value, property.Key, format,charset,source.Properties, Utility.Encode);
                if (string.IsNullOrEmpty(value)) { continue; }

                target.Properties.Add(property.Key, value);
            }

            if (source.Delay > 0L)
            {
                if (source.IsOffsetDelay)
                {
                    Utility.VerifyDelayInRange(source.Delay);
                    target.StartDeliverTime = DateTimeOffset.Now.ToUnixTimeMilliseconds() + source.Delay * 1000;
                }
                else
                {
                    Utility.VerifyDelayInRange(source.Delay - DateTimeOffset.Now.ToUnixTimeSeconds());
                    target.StartDeliverTime = source.Delay * 1000;
                }
            }

            return target;
        }
    }
}