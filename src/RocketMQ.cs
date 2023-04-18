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
        private readonly IServiceProvider services;



        public string Name { get; private set; }

        public RocketMQ(IServiceProvider services) : this(nameof(RocketMQ), services) { }

        public RocketMQ(string name, IServiceProvider services)
        {
            Name = name;
            this.services = services;
            logger = services.GetService<ILogger<RocketMQ>>();
            var configuration = services.GetRequiredService<IConfiguration>().GetSection(name);

            var ak = configuration.GetValue<string>("AK", null);
            var sk = configuration.GetValue<string>("SK", null);
            var endpoint = configuration.GetValue<string>("Endpoint", null);
            client = new(ak, sk, endpoint);
        }

        public IProducer CreateProducer(string name)
        {
            return new Producer(name, Name, services, (configuration) =>
            {
                var instance = configuration.GetValue<string>($"Instance", null);
                var topic = configuration.GetValue<string>($"Topic", null);
                return client.GetProducer(instance, topic);
            });
        }

        public IConsumer CreateConsumer(string name)
        {
            return new Consumer(name, Name, services, (configuration) =>
            {
                var instance = configuration.GetValue<string>($"Instance", null);
                var topic = configuration.GetValue<string>($"Topic", null);
                var tag = configuration.GetValue<string>($"Tag", null);
                var group = configuration.GetValue<string>($"Group", null);
                return client.GetConsumer(instance, topic, group, tag);
            }, (configuration) => {
                var instance = configuration.GetValue<string>($"Instance", null);
                var topic = configuration.GetValue<string>($"Topic", null);
                return client.GetProducer(instance, topic);
            });
        }
    }
}
