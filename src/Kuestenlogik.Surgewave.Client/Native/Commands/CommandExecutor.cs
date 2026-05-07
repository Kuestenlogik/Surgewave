using System.Buffers;
using Kuestenlogik.Surgewave.Protocol.Native;
using Kuestenlogik.Surgewave.Protocol.Native.Serialization;

namespace Kuestenlogik.Surgewave.Client.Native.Commands;

/// <summary>
/// Executes Surgewave commands with unified request/response handling.
/// Eliminates boilerplate in operation classes.
/// </summary>
public sealed class CommandExecutor
{
    private readonly SurgewaveNativeClient _client;

    public CommandExecutor(SurgewaveNativeClient client)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
    }

    /// <summary>
    /// Execute a command and return the result.
    /// </summary>
    public async Task<TResult> ExecuteAsync<TResult>(
        ISurgewaveCommand<TResult> command,
        CancellationToken cancellationToken = default)
    {
        // Estimate buffer size and rent from pool
        var estimatedSize = command.EstimateRequestSize();
        var payloadBuffer = estimatedSize > 0
            ? ArrayPool<byte>.Shared.Rent(estimatedSize)
            : [];

        try
        {
            // Write request payload
            var payloadMemory = ReadOnlyMemory<byte>.Empty;
            if (estimatedSize > 0)
            {
                var writer = new SurgewavePayloadWriter(payloadBuffer);
                command.WriteRequest(ref writer);
                payloadMemory = payloadBuffer.AsMemory(0, writer.Position);
            }

            // Send request and receive response
            var (header, responsePayload) = await _client.SendRequestAsync(
                command.OpCode,
                payloadMemory,
                cancellationToken);

            // Check for protocol errors
            if (header.ErrorCode != SurgewaveErrorCode.None)
            {
                throw new SurgewaveProtocolException(header.ErrorCode, command.OpCode);
            }

            // Parse and return response
            var reader = new SurgewavePayloadReader(responsePayload.Span);
            return command.ReadResponse(ref reader, header);
        }
        finally
        {
            if (estimatedSize > 0)
            {
                ArrayPool<byte>.Shared.Return(payloadBuffer);
            }
        }
    }

    /// <summary>
    /// Execute a void command (no return value).
    /// </summary>
    public async Task ExecuteVoidAsync(
        ISurgewaveVoidCommand command,
        CancellationToken cancellationToken = default)
    {
        await ExecuteAsync(command, cancellationToken);
    }

    /// <summary>
    /// Execute multiple commands in parallel.
    /// </summary>
    public async Task<TResult[]> ExecuteManyAsync<TResult>(
        IEnumerable<ISurgewaveCommand<TResult>> commands,
        CancellationToken cancellationToken = default)
    {
        var tasks = commands.Select(cmd => ExecuteAsync(cmd, cancellationToken));
        return await Task.WhenAll(tasks);
    }
}
