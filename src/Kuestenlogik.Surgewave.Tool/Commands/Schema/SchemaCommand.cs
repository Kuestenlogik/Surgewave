using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Schema;

/// <summary>
/// Command for managing Schema Registry (surgewave schema ...)
/// </summary>
public class SchemaCommand : CommandBase
{
    public SchemaCommand() : base("schema", "Manage Schema Registry")
    {
        Subcommands.Add(new ListSubjectsCommand());
        Subcommands.Add(new DescribeSubjectCommand());
        Subcommands.Add(new RegisterSchemaCommand());
        Subcommands.Add(new GetSchemaCommand());
        Subcommands.Add(new DeleteSubjectCommand());
        Subcommands.Add(new DeleteVersionCommand());
        Subcommands.Add(new CompatibilityCommand());
        Subcommands.Add(new SchemaTypesCommand());
    }
}
