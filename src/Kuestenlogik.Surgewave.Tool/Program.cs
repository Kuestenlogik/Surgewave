using System.CommandLine;
using System.Text;
using Kuestenlogik.Surgewave.Cli;
using Kuestenlogik.Surgewave.Cli.Commands.Acls;
using Kuestenlogik.Surgewave.Cli.Commands.Backup;
using Kuestenlogik.Surgewave.Cli.Commands.Benchmark;
using Kuestenlogik.Surgewave.Cli.Commands.Broker;
using Kuestenlogik.Surgewave.Cli.Commands.Chat;
using Kuestenlogik.Surgewave.Cli.Commands.Cluster;
using Kuestenlogik.Surgewave.Cli.Commands.Completion;
using Kuestenlogik.Surgewave.Cli.Commands.Config;
using Kuestenlogik.Surgewave.Cli.Commands.Connect;
using Kuestenlogik.Surgewave.Cli.Commands.Consume;
using Kuestenlogik.Surgewave.Cli.Commands.Dlq;
using Kuestenlogik.Surgewave.Cli.Commands.Copy;
using Kuestenlogik.Surgewave.Cli.Commands.Groups;
using Kuestenlogik.Surgewave.Cli.Commands.Health;
using Kuestenlogik.Surgewave.Cli.Commands.Logs;
using Kuestenlogik.Surgewave.Cli.Commands.Link;
using Kuestenlogik.Surgewave.Cli.Commands.Messages;
using Kuestenlogik.Surgewave.Cli.Commands.Mirror;
using Kuestenlogik.Surgewave.Cli.Commands.Partitions;
using Kuestenlogik.Surgewave.Cli.Commands.Plugins;
using Kuestenlogik.Surgewave.Cli.Commands.Sdk;
using Kuestenlogik.Surgewave.Cli.Commands.Produce;
using Kuestenlogik.Surgewave.Cli.Commands.Schema;
using Kuestenlogik.Surgewave.Cli.Commands.Templates;
using Kuestenlogik.Surgewave.Cli.Commands.Topics;
using Kuestenlogik.Surgewave.Cli.Commands.Quotas;
using Kuestenlogik.Surgewave.Cli.Commands.Transactions;
using Kuestenlogik.Surgewave.Cli.Commands.Transport;

// Set UTF-8 encoding for proper emoji/unicode display
Console.OutputEncoding = Encoding.UTF8;
Console.InputEncoding = Encoding.UTF8;

// Create root command
var rootCommand = new RootCommand("Surgewave - Kafka-compatible streaming platform CLI");

// Add global options that propagate to all subcommands
GlobalOptions.BootstrapServers.Recursive = true;
GlobalOptions.Verbose.Recursive = true;
GlobalOptions.Format.Recursive = true;
GlobalOptions.Timeout.Recursive = true;
GlobalOptions.AssumeYes.Recursive = true;
rootCommand.Options.Add(GlobalOptions.BootstrapServers);
rootCommand.Options.Add(GlobalOptions.Verbose);
rootCommand.Options.Add(GlobalOptions.Format);
rootCommand.Options.Add(GlobalOptions.Timeout);
rootCommand.Options.Add(GlobalOptions.AssumeYes);

// Add commands
rootCommand.Subcommands.Add(new TopicsCommand());
rootCommand.Subcommands.Add(new PartitionsCommand());
rootCommand.Subcommands.Add(new GroupsCommand());
rootCommand.Subcommands.Add(new BrokerCommand());
rootCommand.Subcommands.Add(new ProduceCommand());
rootCommand.Subcommands.Add(new ConsumeCommand());
rootCommand.Subcommands.Add(new CopyCommand());
rootCommand.Subcommands.Add(new ConfigCommand());
rootCommand.Subcommands.Add(new CompletionCommand());
rootCommand.Subcommands.Add(new HealthCommand());
rootCommand.Subcommands.Add(new BenchmarkCommand());
rootCommand.Subcommands.Add(new ClusterCommand());
rootCommand.Subcommands.Add(new LogsCommand());
rootCommand.Subcommands.Add(new TransactionsCommand());
rootCommand.Subcommands.Add(new QuotasCommand());
rootCommand.Subcommands.Add(new AclsCommand());
rootCommand.Subcommands.Add(new BackupCommand());
rootCommand.Subcommands.Add(new SchemaCommand());
rootCommand.Subcommands.Add(new ConnectCommand());
rootCommand.Subcommands.Add(new TransportCommand());
rootCommand.Subcommands.Add(new LinkCommand());
rootCommand.Subcommands.Add(new MirrorCommand());
rootCommand.Subcommands.Add(new DlqCommand());
rootCommand.Subcommands.Add(new PluginCommand());
rootCommand.Subcommands.Add(new SdkCommand());
rootCommand.Subcommands.Add(new TemplateCommand());
rootCommand.Subcommands.Add(new ChatCommand());
rootCommand.Subcommands.Add(new MessageCommand());

// Discover CLI plugins from plugins/ directory
foreach (var command in Kuestenlogik.Surgewave.Cli.CliPluginDiscovery.DiscoverCommands("plugins"))
{
    rootCommand.Subcommands.Add(command);
}

// Run the CLI
return await rootCommand.Parse(args).InvokeAsync();
