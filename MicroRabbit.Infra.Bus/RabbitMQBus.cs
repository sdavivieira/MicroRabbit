using MediatR;
using MicroRabbit.Domain.Core.Bus;
using MicroRabbit.Domain.Core.Commands;
using MicroRabbit.Domain.Core.Events;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http.Json;
using System.Text;
using System.Text.Json.Serialization;
using System.Threading.Tasks;

namespace MicroRabbit.Infra.Bus
{
    public sealed class RabbitMQBus : IEventBus
    {
        private readonly IMediator _mediator;
        private readonly Dictionary<string, List<Type>> _handlers;
        private readonly List<Type> _eventTypes;

        public RabbitMQBus(IMediator mediator)
        {
            _mediator = mediator;
            _handlers = new Dictionary<string, List<Type>>();
            _eventTypes = new List<Type>();
        }

        public Task SendCommand<T>(T command) where T : Command
        {
            return _mediator.Send(command);
        }

        public async Task Publish<T>(T @event) where T : Event
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            var eventName = @event.GetType().Name;
            await channel.QueueDeclareAsync(eventName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var message = JsonConvert.SerializeObject(@event);
            var body = Encoding.UTF8.GetBytes(message);

            var properties = new BasicProperties
            {
                Persistent = true
            };

            await channel.BasicPublishAsync(exchange: "", routingKey: eventName, mandatory: false, basicProperties: properties, body: body);
        }

        public async Task Subscribe<T, TH>()
                where T : Event
                where TH : IEventHandler<T>
            {
                var eventName = typeof(T).Name;
                var handlerType = typeof(TH);

                if (!_eventTypes.Contains(typeof(T)))
                {
                    _eventTypes.Add(typeof(T));
                }

                if (!_handlers.ContainsKey(eventName))
                {
                    _handlers.Add(eventName, new List<Type>());
                }

                if (_handlers[eventName].Any(s => s.GetType() == handlerType))
                {
                    throw new ArgumentException(
                        $"handler type {handlerType.Name} already is registered for '{eventName}", nameof(handlerType));
                }

                _handlers[eventName].Add(handlerType);

               await StartBasicConsume<T>();
        }

        private async Task StartBasicConsume<T>() where T : Event
        {
            var factory = new ConnectionFactory { HostName = "localhost" };

            await using var connection = await factory.CreateConnectionAsync();
            await using var channel = await connection.CreateChannelAsync();

            var eventName = typeof(T).Name;
            await channel.QueueDeclareAsync(eventName, durable: false, exclusive: false, autoDelete: false, arguments: null);

            var consumer = new AsyncEventingBasicConsumer(channel);
            consumer.ReceivedAsync += Consumer_Received;

            await channel.BasicConsumeAsync(eventName, true, consumer);

        }

        private async Task Consumer_Received(object sender, BasicDeliverEventArgs e)
        {
            var eventName = e.RoutingKey;
            var message = Encoding.UTF8.GetString(e.Body.ToArray());

            try
            {
                await ProcessEvent(eventName, message).ConfigureAwait(false);
            }

            catch (Exception ex)
            { 
                
            }
        }

        private async Task ProcessEvent(string eventName, string message)
        {
            if (_handlers.ContainsKey(eventName))
            {
                var subscriptions = _handlers[eventName];
                foreach (var subscription in subscriptions)
                {
                    var handler = Activator.CreateInstance(subscription);
                    if (handler == null) continue;

                    var eventType = _eventTypes.SingleOrDefault(t => t.Name == eventName);
                    if (eventType == null) continue;

                    var @event = JsonConvert.DeserializeObject(message, eventType);
                    if (@event == null) continue;

                    var concreteType = typeof(IEventHandler<>).MakeGenericType(eventType);
                    var method = concreteType.GetMethod("Handle");

                    if (method == null) continue;

                    await (Task)method.Invoke(handler, new object[] { @event });
                }
            }
        }

    }
}
