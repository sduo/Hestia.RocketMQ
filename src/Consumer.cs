using Aliyun.MQ;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using Hestia.MQ.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using HestiaMessage = Hestia.MQ.Abstractions.Message;
using RocketMessage = Aliyun.MQ.Model.Message;

namespace Hestia.RocketMQ
{
    public class Consumer : IConsumer
    {
        public string Name { get; private set; }

        private readonly ILogger<Producer> logger;
        private readonly IConfiguration configuration;
        private readonly MQConsumer consumer;
        private readonly MQProducer producer;
        private readonly uint batch;
        private readonly uint timeout;

        private readonly string formatPrefix;
        private readonly string charsetPrefix;

        private readonly string idempotent;

        public Consumer(string name,string mq,IServiceProvider services ,Func<IConfiguration,MQConsumer> consumerFactory, Func<IConfiguration,MQProducer> producerFactory)
        {
            Name = name;
            logger = services.GetService<ILogger<Producer>>();
            configuration = services.GetRequiredService<IConfiguration>().GetSection($"{mq}:Consumer:{name}");
            consumer = consumerFactory.Invoke(configuration);
            producer = producerFactory.Invoke(configuration);

            batch = configuration.GetValue("Batch", 1u);
            timeout = configuration.GetValue("Timeout", 5u);

            formatPrefix = configuration.GetValue("FormatPrefix", Utility.DefaultFormatPrefix);
            charsetPrefix = configuration.GetValue("CharsetPrefix", Utility.DefaultCharsetPrefix);

            idempotent = configuration.GetValue<string>("Idempotent", null);
        }
        

        [Obsolete("Consume(Func<HestiaMessage, long> callback)")]
        public void Consume(Action<HestiaMessage> callback)
        {
            Consume((message) =>
            {
                callback(message);
                return 0;
            });
        }
        [Obsolete("Consume(Func<HestiaMessage, long> callback)")]
        public void Consume(Func<HestiaMessage, bool> callback)
        {
            Consume((message) =>
            {
                var result = callback(message);
                if (result) { return 0L; }
                return Random.Shared.NextInt64(5L, 30L);
            });
        }

        private List<RocketMessage> Ack(List<RocketMessage> messages)
        {
            List<string> handlers = messages.Select(x => x.ReceiptHandle).ToList();
            try
            {
                consumer.AckMessage(handlers);
                return messages;
            }
            catch (AckMessageException ex)
            {
                foreach(var error in ex.ErrorItems)
                {
                    Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<BadAck>{consumer.IntanceId}/{consumer.TopicName}/{consumer.Consumer}/{consumer.MessageTag}{messages.FirstOrDefault(x => x.Equals(error.ReceiptHandle))?.Id ?? $"ReceiptHandle:{error.ReceiptHandle}"}/[{error.ErrorCode}]{error.ErrorMessage}({ex.RequestId})");
                }                
                return messages.Where(x => !ex.ErrorItems.Any(error => x.ReceiptHandle.Equals(error.ReceiptHandle))).ToList();
            }
        }

        public void Consume(Func<HestiaMessage, long> callback)
        {
            List<RocketMessage> messages = new();
            try
            {
                messages.AddRange(consumer.ConsumeMessage(batch, timeout));
                foreach (RocketMessage message in messages)
                {
                    Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<Consume>{consumer.IntanceId}/{consumer.TopicName}/{consumer.Consumer}/{consumer.MessageTag}/{message.Id}({message.MessageTag})");
                }                
            }
            catch (MessageNotExistException ex)
            {
                Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<Consume>{consumer.IntanceId}/{consumer.TopicName}/{consumer.Consumer}/{consumer.MessageTag}=>{ex.Message}({ex.RequestId})");
            }

            if (messages.Count == 0)
            {
                return;
            }

            var available = Ack(messages);

            foreach (var current in available)
            {
                var target = current.GetProperty(idempotent, null);
                if(!string.IsNullOrEmpty(target) && !string.Equals("*",target) && !string.Equals(consumer.Consumer,target))
                {
                    Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<Drop>{consumer.IntanceId}/{consumer.TopicName}/{consumer.Consumer}/{consumer.MessageTag}/{current.Id}({current.MessageTag}:{target})");
                    continue;
                }
                var message = BuildConsumeMessage(current);
                var result = callback(message);
                if (result > 0)
                {
                    var request = BuildRetryMessage(current, result);
                    if (!string.IsNullOrEmpty(idempotent))
                    {
                        request.Properties.Add(idempotent, consumer.Consumer);
                    }

                    var response = producer.PublishMessage(request);
                    Trace.WriteLine($"[{DateTimeOffset.Now:yyyy-MM-dd HH:mm:ss.fff}:{Environment.CurrentManagedThreadId}]<RePublish>{consumer.IntanceId}/{consumer.TopicName}/{consumer.Consumer}/{consumer.MessageTag}/{current.Id}=>{producer.IntanceId}/{producer.TopicName}/{response.Id}({current.MessageTag})");
                }

            }
        }

        internal TopicMessage BuildRetryMessage(RocketMessage source, long delay)
        {
            Utility.VerifyDelayInRange(delay);
            var target = new TopicMessage(source.Body, source.MessageTag);

            if (!string.IsNullOrEmpty(source.MessageKey))
            {
                target.MessageKey = source.MessageKey;
            }

            if (!string.IsNullOrEmpty(source.ShardingKey))
            {
                target.ShardingKey = source.ShardingKey;
            }
            
            foreach (var injector in Utility.RetryMessagePropertyInjector)
            {
                if (string.IsNullOrEmpty(injector.Key)) { continue; }
                var value = injector.Value.Invoke(source);
                if (string.IsNullOrEmpty(value)) { continue; }
                target.Properties.Add(injector.Key, value);
            }

            foreach (var property in source.Properties)
            {
                if (string.IsNullOrEmpty(property.Key) || string.IsNullOrEmpty(property.Value)) { continue; }
                if (Utility.RetryMessagePropertyInjector.ContainsKey(property.Key)) { continue; }

                if (property.Key.StartsWith(formatPrefix) || property.Key.StartsWith(charsetPrefix)) { target.Properties.Add(property.Key, property.Value); }

                var value = Utility.RetryMessagePropertyMapper.ContainsKey(property.Key) ? Utility.RetryMessagePropertyMapper[property.Key].Invoke(property.Value) : property.Value;
                if (string.IsNullOrEmpty(value)) { continue; }

                target.Properties.Add(property.Key, value);
            }

            target.StartDeliverTime = DateTimeOffset.Now.ToUnixTimeMilliseconds() + delay * 1000;

            return target;
        }

        internal HestiaMessage BuildConsumeMessage(RocketMessage source)
        {
            var target = new HestiaMessage
            {
                Id = source.Id,
                Body = Utility.Transform(source.Body, () => {
                    var key = string.Concat(formatPrefix, nameof(RocketMessage.Body));
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, () => {
                    var key = string.Concat(charsetPrefix, nameof(RocketMessage.Body));
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, Utility.Decode),
                Tag = source.MessageTag,
                Key = source.MessageKey
            };
            if (!string.IsNullOrEmpty(source.ShardingKey))
            {
                target.Properties.Add(nameof(RocketMessage.ShardingKey), source.ShardingKey);
            }

            foreach (var injector in Utility.ConsumeMessageSdkPropertyInjector)
            {
                if (string.IsNullOrEmpty(injector.Key)) { continue; }
                var value = injector.Value.Invoke(source);
                if (string.IsNullOrEmpty(value)) { continue; }
                target.Properties.Add(injector.Key, value);
            }

            foreach (var injector in Utility.ConsumeMessagePropertyInjector)
            {
                if (string.IsNullOrEmpty(injector.Key)) { continue; }
                var value = injector.Value.Invoke(source);
                if (string.IsNullOrEmpty(value)) { continue; }
                target.Properties.Add(injector.Key, value);
            }

            foreach (var property in source.Properties)
            {
                if (string.IsNullOrEmpty(property.Key) || string.IsNullOrEmpty(property.Value)) { continue; }
                if (Utility.ConsumeMessageSdkPropertyInjector.ContainsKey(property.Key)) { continue; }
                if (Utility.ConsumeMessagePropertyInjector.ContainsKey(property.Key)) { continue; }

                if (property.Key.StartsWith(formatPrefix) || property.Key.StartsWith(charsetPrefix)) { target.Properties.Add(property.Key, property.Value); }

                var value = Utility.ConsumeMessagePropertyMapper.ContainsKey(property.Key) ? Utility.ConsumeMessagePropertyMapper[property.Key].Invoke(property.Value) : Utility.Transform(property.Value, () => {
                    var key = string.Concat(formatPrefix, property.Key);
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, () => {
                    var key = string.Concat(charsetPrefix, property.Key);
                    return Utility.GetFromDictionary(source.Properties, key, configuration.GetValue<string>(key, null));
                }, Utility.Decode);
                if (string.IsNullOrEmpty(value)) { continue; }

                target.Properties.Add(property.Key, value);
            }

            return target;
        }


    }
}
