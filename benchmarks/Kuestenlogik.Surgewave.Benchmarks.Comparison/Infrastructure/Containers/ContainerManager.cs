using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Testcontainers.Kafka;
using Testcontainers.Redpanda;

namespace Kuestenlogik.Surgewave.Benchmarks.Comparison.Infrastructure;

/// <summary>
/// Manages Docker containers for benchmarks using Testcontainers.NET.
/// Provides automatic lifecycle management for Kafka, Redpanda, and Surgewave containers.
/// </summary>
public static class ContainerManager
{
    private static KafkaContainer? _kafkaContainer;
    private static RedpandaContainer? _redpandaContainer;
    private static IContainer? _surgewaveContainer;
    private static string? _surgewaveContainerBootstrap;
    private static readonly SemaphoreSlim _lock = new(1, 1);

    /// <summary>
    /// Gets the Kafka bootstrap servers. Starts the container if not already running.
    /// </summary>
    public static async Task<string> GetKafkaBootstrapServersAsync(string image = "confluentinc/cp-kafka:7.6.0")
    {
        await _lock.WaitAsync();
        try
        {
            if (_kafkaContainer == null)
            {
                Console.WriteLine($"Starting Kafka container ({image})...");
                _kafkaContainer = new KafkaBuilder(image)
                    .Build();
                await _kafkaContainer.StartAsync();
                Console.WriteLine($"Kafka started: {_kafkaContainer.GetBootstrapAddress()}");
            }
            return _kafkaContainer.GetBootstrapAddress();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets the Redpanda bootstrap servers. Starts the container if not already running.
    /// </summary>
    public static async Task<string> GetRedpandaBootstrapServersAsync(string image = "redpandadata/redpanda:latest")
    {
        await _lock.WaitAsync();
        try
        {
            if (_redpandaContainer == null)
            {
                Console.WriteLine($"Starting Redpanda container ({image})...");
                _redpandaContainer = new RedpandaBuilder(image)
                    .Build();
                await _redpandaContainer.StartAsync();
                Console.WriteLine($"Redpanda started: {_redpandaContainer.GetBootstrapAddress()}");
            }
            return _redpandaContainer.GetBootstrapAddress();
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets the Surgewave container bootstrap servers. Starts the container if not already running.
    /// Exposes Kafka-compatible port 9092.
    /// </summary>
    public static async Task<string> GetSurgewaveContainerBootstrapAsync(string image = "surgewave:latest")
    {
        await _lock.WaitAsync();
        try
        {
            if (_surgewaveContainer == null)
            {
                Console.WriteLine($"Starting Surgewave container ({image})...");
                _surgewaveContainer = new ContainerBuilder()
                    .WithImage(image)
                    .WithPortBinding(9092, true)
                    .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Broker started"))
                    .Build();
                await _surgewaveContainer.StartAsync();

                var host = _surgewaveContainer.Hostname;
                var port = _surgewaveContainer.GetMappedPublicPort(9092);
                _surgewaveContainerBootstrap = $"{host}:{port}";
                Console.WriteLine($"Surgewave container started: {_surgewaveContainerBootstrap}");
            }
            return _surgewaveContainerBootstrap!;
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Gets the Redpanda Schema Registry address. Starts the container if not already running.
    /// </summary>
    public static async Task<string> GetRedpandaSchemaRegistryAsync()
    {
        await GetRedpandaBootstrapServersAsync();
        return _redpandaContainer!.GetSchemaRegistryAddress();
    }

    /// <summary>
    /// Stops all running containers. Should be called at the end of benchmarks.
    /// </summary>
    public static async Task StopAllAsync()
    {
        await _lock.WaitAsync();
        try
        {
            if (_kafkaContainer != null)
            {
                Console.WriteLine("Stopping Kafka container...");
                await _kafkaContainer.DisposeAsync();
                _kafkaContainer = null;
            }

            if (_redpandaContainer != null)
            {
                Console.WriteLine("Stopping Redpanda container...");
                await _redpandaContainer.DisposeAsync();
                _redpandaContainer = null;
            }

            if (_surgewaveContainer != null)
            {
                Console.WriteLine("Stopping Surgewave container...");
                await _surgewaveContainer.DisposeAsync();
                _surgewaveContainer = null;
                _surgewaveContainerBootstrap = null;
            }
        }
        finally
        {
            _lock.Release();
        }
    }

    /// <summary>
    /// Checks if a broker is available at the given address without starting a container.
    /// </summary>
    public static async Task<bool> IsKafkaAvailableAsync(string bootstrapServers, TimeSpan timeout)
    {
        try
        {
            var parts = bootstrapServers.Split(':');
            if (parts.Length != 2 || !int.TryParse(parts[1], out var port))
                return false;

            using var client = new System.Net.Sockets.TcpClient();
            var connectTask = client.ConnectAsync(parts[0], port);
            var timeoutTask = Task.Delay(timeout);

            var completed = await Task.WhenAny(connectTask, timeoutTask);
            return completed == connectTask && client.Connected;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Checks whether Docker is available on this host.
    /// </summary>
    public static async Task<bool> IsDockerAvailableAsync()
    {
        try
        {
            var process = System.Diagnostics.Process.Start(new System.Diagnostics.ProcessStartInfo
            {
                FileName = "docker",
                Arguments = "info",
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
                CreateNoWindow = true
            });

            if (process == null) return false;
            await process.WaitForExitAsync();
            return process.ExitCode == 0;
        }
        catch
        {
            return false;
        }
    }

    /// <summary>
    /// Gets Kafka bootstrap servers, using the provided one if available, or starting a container.
    /// </summary>
    public static async Task<string> GetOrStartKafkaAsync(string? preferredBootstrapServers = null, string image = "confluentinc/cp-kafka:7.6.0")
    {
        if (!string.IsNullOrEmpty(preferredBootstrapServers))
        {
            if (await IsKafkaAvailableAsync(preferredBootstrapServers, TimeSpan.FromSeconds(2)))
            {
                Console.WriteLine($"Using existing Kafka at {preferredBootstrapServers}");
                return preferredBootstrapServers;
            }
            Console.WriteLine($"Kafka not available at {preferredBootstrapServers}, starting container...");
        }

        return await GetKafkaBootstrapServersAsync(image);
    }

    /// <summary>
    /// Gets Redpanda bootstrap servers, using the provided one if available, or starting a container.
    /// </summary>
    public static async Task<string> GetOrStartRedpandaAsync(string? preferredBootstrapServers = null, string image = "redpandadata/redpanda:latest")
    {
        if (!string.IsNullOrEmpty(preferredBootstrapServers))
        {
            if (await IsKafkaAvailableAsync(preferredBootstrapServers, TimeSpan.FromSeconds(2)))
            {
                Console.WriteLine($"Using existing Redpanda at {preferredBootstrapServers}");
                return preferredBootstrapServers;
            }
            Console.WriteLine($"Redpanda not available at {preferredBootstrapServers}, starting container...");
        }

        return await GetRedpandaBootstrapServersAsync(image);
    }
}
