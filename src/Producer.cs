using Aliyun.MQ;
using Aliyun.MQ.Model;
using Hestia.MQ.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Diagnostics;
using HestiaMessage = Hestia.MQ.Abstractions.Message;

namespace Hestia.RocketMQ
{
    public class Producer : IProducer
    {
        public string Name { get; private set; }
        private readonly ILogger<Producer> logger;
        private readonly IConfiguration configuration;

        private readonly MQProducer producer;
        private readonly string formatPrefix = null;
        private readonly string charsetPrefix = null;

        public Producer(string name, string mq,IServiceProvider services,Func<IConfiguration,MQProducer> producerFactory)
        {
            Name = name;
            logger = services.GetService<ILogger<Producer>>();
            configuration = services.GetRequiredService<IConfiguration>().GetSection($"{mq}:Producer:{name}");
            producer = producerFactory.Invoke(configuration);

            formatPrefix = configuration.GetValue<string>("FormatPrefix", null);
            charsetPrefix = configuration.GetValue<string>("CharsetPrefix", null);
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
            var target = new TopicMessage(Utility.Transform(source.Body, () => {
                var key = string.Concat(formatPrefix, nameof(HestiaMessage.Body));
                return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
            }, () => {
                var key = string.Concat(charsetPrefix, nameof(HestiaMessage.Body));
                return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
            },Utility.Encode), source.Tag);

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
                if (property.Key.StartsWith(formatPrefix) || property.Key.StartsWith(charsetPrefix)) { target.Properties.Add(property.Key, property.Value); }

                var value = Utility.PublishMessagePropertyMapper.ContainsKey(property.Key) ? Utility.PublishMessagePropertyMapper[property.Key].Invoke(property.Value) : Utility.Transform(property.Value, () => {
                    var key = string.Concat(formatPrefix, property.Key);
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, () => {
                    var key = string.Concat(charsetPrefix, property.Key);
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, Utility.Encode);
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