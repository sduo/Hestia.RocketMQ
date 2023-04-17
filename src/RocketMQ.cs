using Aliyun.MQ;
using Hestia.MQ.Abstractions;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;

namespace Hestia.RocketMQ
{
    public class RocketMQ : IMessageQueue
    {
        private readonly MQClient client;
        private readonly ILogger<RocketMQ> logger;
        private readonly IConfiguration configuration;
       


        public string Name { get; private set; }

        public RocketMQ(IServiceProvider services) : this(nameof(RocketMQ), services) { }

        public RocketMQ(string name, IServiceProvider services)
        {
            Name = name;
            logger = services.GetService<ILogger<RocketMQ>>();
            configuration = services.GetRequiredService<IConfiguration>().GetSection(name);
            var ak = configuration.GetValue<string>("AK", null);
            var sk = configuration.GetValue<string>("SK", null);
            var endpoint = configuration.GetValue<string>("Endpoint", null);
            client = new(ak, sk, endpoint);
        }

        public IProducer CreateProducer(string name)
        {
            var instance = configuration.GetValue<string>($"Producer:{name}:Instance", null);
            var topic = configuration.GetValue<string>($"Producer:{name}:Topic", null);
            var producer = client.GetProducer(instance, topic);
            var format = configuration.GetValue<string>("Format", null);
            var charset = configuration.GetValue<string>("Charset", null);
            return new Producer(producer, format, charset);
        }

        public IConsumer CreateConsumer(string name)
        {
            var instance = configuration.GetValue<string>($"Consumer:{name}:Instance", null);
            var topic = configuration.GetValue<string>($"Consumer:{name}:Topic", null);
            var tag = configuration.GetValue<string>($"Consumer:{name}:Tag", null);
            var group = configuration.GetValue<string>($"Consumer:{name}:Group", null);
            var producer = client.GetProducer(instance, topic);
            var consumer = client.GetConsumer(instance, topic, group, tag);
            var format = configuration.GetValue<string>("FormatPrefix", null);
            var charset = configuration.GetValue<string>("CharsetPrefix", null);
            var batch = configuration.GetValue<uint?>("Batch", null);
            var timeout = configuration.GetValue<uint?>("Timeout", null);
            var gid = configuration.GetValue<string>("GroupIdName", null);
            return new Consumer(producer, consumer, format, charset, batch,timeout, gid);
        }

        
    }
}
