using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Xunit;

namespace Level79.Common.Test;

public abstract class IntegrationTest
{
    private readonly TimeSpan _fromSeconds = TimeSpan.FromSeconds(30);

    [Fact]
    [Trait("Category", "IntegrationTest")]
    public virtual async Task Integration()
    {
        await ExecuteWithStatistics(1, 1, TimeSpan.FromSeconds(10));
    }

    [Fact]
    [Trait("Category", "PerformanceTest")]
    public virtual async Task Performance()
    {
        await ExecuteWithStatistics(1000, 1, TimeSpan.FromSeconds(10));
        await ExecuteWithStatistics(1000, 10, TimeSpan.FromSeconds(10));
    }

    private async Task ExecuteWithStatistics(int maxExecutions, int parallelTests, TimeSpan timeLimit)
    {
        var cts = new CancellationTokenSource(timeLimit);
        var executions = 0;
        var stopwatch = Stopwatch.StartNew();
        try
        {
            var parallelOptions = new ParallelOptions
            {
                MaxDegreeOfParallelism = parallelTests,
                CancellationToken = cts.Token
            };

            await Parallel.ForEachAsync(Enumerable.Range(0, maxExecutions), parallelOptions, async (_, token) =>
            {
                if (token.IsCancellationRequested) return;
                await Execute(token);
                Interlocked.Increment(ref executions);
            });
        }
        catch (TaskCanceledException)
        {
        }
        finally
        {
            stopwatch.Stop();
            var elapsedSeconds = stopwatch.ElapsedMilliseconds / 1000;
            var executionsPerSeconds = executions / (stopwatch.ElapsedMilliseconds / 1000);
            Console.WriteLine($"Tests run {executions} times in {elapsedSeconds} seconds at {executionsPerSeconds} per second");
        }
    }

    protected abstract Task Execute(CancellationToken cancellationToken);
}