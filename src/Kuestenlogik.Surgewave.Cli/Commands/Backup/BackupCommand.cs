using System.CommandLine;

namespace Kuestenlogik.Surgewave.Cli.Commands.Backup;

/// <summary>
/// Parent command for backup operations (surgewave backup ...)
/// </summary>
public class BackupCommand : CommandBase
{
    public BackupCommand() : base("backup", "Backup and restore operations")
    {
        Subcommands.Add(new CreateBackupCommand());
        Subcommands.Add(new RestoreBackupCommand());
        Subcommands.Add(new ListBackupsCommand());
        Subcommands.Add(new VerifyBackupCommand());
    }
}
