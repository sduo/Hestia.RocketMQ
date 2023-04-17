using Aliyun.MQ;
using Aliyun.MQ.Model;
using Aliyun.MQ.Model.Exp;
using Hestia.MQ.Abstractions;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using RocketMessage = Aliyun.MQ.Model.Message;
using HestiaMessage = Hestia.MQ.Abstractions.Message;

namespace Hestia.RocketMQ
{
    public class Consumer : IConsumer
    {
        public const uint DefaultBatch = 1u;
        public const uint DefaultTimeout = 5u;
        public const string DefaultGroupId = "GID";

        public readonly MQConsumer consumer;
        public readonly MQProducer producer;

        private readonly string format = null;
        private readonly string charset = null;
        private readonly string gid = null;

        private readonly uint batch;
        private readonly uint timeout;

        public Consumer(MQProducer producer, MQConsumer consumer,string format,string charset,uint? batch,uint? timeout,string gid)
        {
            this.producer = producer;
            this.consumer = consumer;
            this.format = format ?? Utility.DefaultFormatPrefix;
            this.charset = charset ?? Utility.DefaultCharsetPrefix;
            this.batch = batch ?? DefaultBatch;
            this.timeout = timeout ?? DefaultTimeout;
            this.gid = gid ?? DefaultGroupId;
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
                var target = current.GetProperty(gid, null);
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

                if (property.Key.StartsWith(format) || property.Key.StartsWith(charset)) { target.Properties.Add(property.Key, property.Value); }

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
                Body = Utility.Transform(source.Body, nameof(RocketMessage.Body),format,charset, source.Properties, Utility.Decode),
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

                if (property.Key.StartsWith(format) || property.Key.StartsWith(charset)) { target.Properties.Add(property.Key, property.Value); }

                var value = Utility.ConsumeMessagePropertyMapper.ContainsKey(property.Key) ? Utility.ConsumeMessagePropertyMapper[property.Key].Invoke(property.Value) : Utility.Transform(property.Value, property.Key,format,charset, source.Properties, Utility.Decode);
                if (string.IsNullOrEmpty(value)) { continue; }

                target.Properties.Add(property.Key, value);
            }

            return target;
        }


    }
}
