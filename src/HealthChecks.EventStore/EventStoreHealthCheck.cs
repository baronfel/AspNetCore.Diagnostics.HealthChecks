using EventStore.ClientAPI;
using EventStore.ClientAPI.SystemData;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace HealthChecks.EventStore
{
    public class EventStoreHealthCheck
        : IHealthCheck
    {
        const string CONNECTION_NAME = "AspNetCore HealthCheck Connection";
        const int ELAPSED_DELAY_MILLISECONDS = 500;
        const int RECONNECTION_LIMIT = 1;

        private readonly string _eventStoreConnectionString;
        private readonly ConnectionSettingsBuilder _eventStoreConnectionSettings;

        public EventStoreHealthCheck(string eventStoreConnectionString, string login, string password)
        {
            _eventStoreConnectionString = eventStoreConnectionString ?? throw new ArgumentNullException(nameof(eventStoreConnectionString));
            _eventStoreConnectionSettings = CreateDefaultSettings(login, password);
        }

        public EventStoreHealthCheck(string eventStoreConnectionString, ConnectionSettingsBuilder eventStoreConnectionSettings)
        {
            _eventStoreConnectionString = eventStoreConnectionString ?? throw new ArgumentNullException(nameof(eventStoreConnectionString));
            _eventStoreConnectionSettings = eventStoreConnectionSettings ?? throw new ArgumentNullException(nameof(eventStoreConnectionSettings));
        }

        private ConnectionSettingsBuilder CreateDefaultSettings(string login, string password) {
            if (string.IsNullOrEmpty(login) || string.IsNullOrEmpty(password))
            {
                return ConnectionSettings.Create()
                    .LimitReconnectionsTo(RECONNECTION_LIMIT)
                    .SetReconnectionDelayTo(TimeSpan.FromMilliseconds(ELAPSED_DELAY_MILLISECONDS));
            }
            else
            {
                return ConnectionSettings.Create()
                    .LimitReconnectionsTo(RECONNECTION_LIMIT)
                    .SetReconnectionDelayTo(TimeSpan.FromMilliseconds(ELAPSED_DELAY_MILLISECONDS))
                    .SetDefaultUserCredentials(new UserCredentials(login, password));
            }
        }

        public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
        {
            try
            {
                using (var connection = EventStoreConnection.Create(
                    _eventStoreConnectionString,
                    _eventStoreConnectionSettings,
                    CONNECTION_NAME))
                {
                    var tcs = new TaskCompletionSource<HealthCheckResult>();

                    //connected
                    connection.Connected += (s, e) =>
                    {
                        tcs.TrySetResult(HealthCheckResult.Healthy());
                    };

                    //connection closed after configured amount of failed reconnections
                    connection.Closed += (s, e) =>
                    {
                        tcs.TrySetResult(new HealthCheckResult(
                            status: context.Registration.FailureStatus,
                            description: e.Reason));
                    };

                    //connection error
                    connection.ErrorOccurred += (s, e) =>
                    {
                        tcs.TrySetResult(new HealthCheckResult(
                            status: context.Registration.FailureStatus,
                            exception: e.Exception));
                    };

                    using (cancellationToken.Register(() => connection.Close()))
                    {
                        //completes after tcp connection init, but before successful connection and login
                        await connection.ConnectAsync();
                    }

                    cancellationToken.ThrowIfCancellationRequested();

                    using (cancellationToken.Register(() => tcs.TrySetCanceled()))
                    {
                        return await tcs.Task;
                    }
                }
            }
            catch (Exception ex)
            {
                return new HealthCheckResult(context.Registration.FailureStatus, exception: ex);
            }
        }
    }
}