using System.CommandLine;
using System.CommandLine.Parsing;
using Spectre.Console;

namespace Kuestenlogik.Surgewave.Cli.Commands.Completion;

/// <summary>
/// Command for generating shell completion scripts (surgewave completion)
/// </summary>
public class CompletionCommand : CommandBase
{
    public CompletionCommand() : base("completion", "Generate shell completion scripts")
    {
        Subcommands.Add(new BashCompletionCommand());
        Subcommands.Add(new PowerShellCompletionCommand());
        Subcommands.Add(new ZshCompletionCommand());
        Subcommands.Add(new FishCompletionCommand());
    }
}

public class BashCompletionCommand : CommandBase
{
    public BashCompletionCommand() : base("bash", "Generate bash completion script")
    {
        this.SetAction(Execute);
    }

    private Task<int> Execute(ParseResult parseResult, CancellationToken ct)
    {
        var script = """
            # Surgewave CLI bash completion
            _surgewave_completions()
            {
                local cur prev words cword
                _init_completion || return

                local commands="topics groups broker produce consume completion"
                local topics_commands="list create delete describe"
                local groups_commands="list describe delete"
                local broker_commands="info health"
                local completion_commands="bash powershell zsh fish"

                case "${words[1]}" in
                    topics)
                        COMPREPLY=($(compgen -W "$topics_commands" -- "$cur"))
                        return
                        ;;
                    groups)
                        COMPREPLY=($(compgen -W "$groups_commands" -- "$cur"))
                        return
                        ;;
                    broker)
                        COMPREPLY=($(compgen -W "$broker_commands" -- "$cur"))
                        return
                        ;;
                    completion)
                        COMPREPLY=($(compgen -W "$completion_commands" -- "$cur"))
                        return
                        ;;
                esac

                if [[ $cword -eq 1 ]]; then
                    COMPREPLY=($(compgen -W "$commands" -- "$cur"))
                fi
            }

            complete -F _surgewave_completions surgewave
            """;

        Console.WriteLine(script);
        AnsiConsole.MarkupLine("\n[dim]# Add to ~/.bashrc:[/]");
        AnsiConsole.MarkupLine("[dim]# source <(surgewave completion bash)[/]");
        return Task.FromResult(0);
    }
}

public class PowerShellCompletionCommand : CommandBase
{
    public PowerShellCompletionCommand() : base("powershell", "Generate PowerShell completion script")
    {
        this.SetAction(Execute);
    }

    private Task<int> Execute(ParseResult parseResult, CancellationToken ct)
    {
        var script = """
            # Surgewave CLI PowerShell completion
            Register-ArgumentCompleter -Native -CommandName surgewave -ScriptBlock {
                param($wordToComplete, $commandAst, $cursorPosition)

                $commands = @{
                    '' = @('topics', 'groups', 'broker', 'produce', 'consume', 'completion')
                    'topics' = @('list', 'create', 'delete', 'describe')
                    'groups' = @('list', 'describe', 'delete')
                    'broker' = @('info', 'health')
                    'completion' = @('bash', 'powershell', 'zsh', 'fish')
                }

                $tokens = $commandAst.CommandElements | Select-Object -Skip 1 | ForEach-Object { $_.ToString() }

                $completions = if ($tokens.Count -eq 0) {
                    $commands['']
                } elseif ($tokens.Count -eq 1 -and $commands.ContainsKey($tokens[0])) {
                    $commands[$tokens[0]]
                } else {
                    @()
                }

                $completions | Where-Object { $_ -like "$wordToComplete*" } | ForEach-Object {
                    [System.Management.Automation.CompletionResult]::new($_, $_, 'ParameterValue', $_)
                }
            }
            """;

        Console.WriteLine(script);
        Console.WriteLineToError("\n# Add to your PowerShell profile:");
        Console.WriteLineToError("# surgewave completion powershell | Out-String | Invoke-Expression");
        return Task.FromResult(0);
    }
}

public class ZshCompletionCommand : CommandBase
{
    public ZshCompletionCommand() : base("zsh", "Generate zsh completion script")
    {
        this.SetAction(Execute);
    }

    private Task<int> Execute(ParseResult parseResult, CancellationToken ct)
    {
        var script = """
            #compdef surgewave

            _surgewave() {
                local -a commands topics_commands groups_commands broker_commands completion_commands

                commands=(
                    'topics:Manage topics'
                    'groups:Manage consumer groups'
                    'broker:Broker information and health'
                    'produce:Produce messages to a topic'
                    'consume:Consume messages from a topic'
                    'completion:Generate shell completion scripts'
                )

                topics_commands=(
                    'list:List all topics'
                    'create:Create a new topic'
                    'delete:Delete a topic'
                    'describe:Describe a topic'
                )

                groups_commands=(
                    'list:List all consumer groups'
                    'describe:Describe a consumer group'
                    'delete:Delete a consumer group'
                )

                broker_commands=(
                    'info:Display broker information'
                    'health:Check broker health'
                )

                completion_commands=(
                    'bash:Generate bash completion script'
                    'powershell:Generate PowerShell completion script'
                    'zsh:Generate zsh completion script'
                    'fish:Generate fish completion script'
                )

                _arguments -C \
                    '--bootstrap-server[Broker address]:server:' \
                    '--verbose[Show detailed output]' \
                    '--format[Output format]:format:(table json plain)' \
                    '1:command:->cmds' \
                    '*::arg:->args'

                case "$state" in
                    cmds)
                        _describe -t commands 'surgewave commands' commands
                        ;;
                    args)
                        case "${words[1]}" in
                            topics) _describe -t topics_commands 'topics commands' topics_commands ;;
                            groups) _describe -t groups_commands 'groups commands' groups_commands ;;
                            broker) _describe -t broker_commands 'broker commands' broker_commands ;;
                            completion) _describe -t completion_commands 'completion commands' completion_commands ;;
                        esac
                        ;;
                esac
            }

            _surgewave "$@"
            """;

        Console.WriteLine(script);
        Console.WriteLineToError("\n# Save to ~/.zsh/completions/_surgewave");
        Console.WriteLineToError("# Add to ~/.zshrc: fpath=(~/.zsh/completions $fpath)");
        return Task.FromResult(0);
    }
}

public class FishCompletionCommand : CommandBase
{
    public FishCompletionCommand() : base("fish", "Generate fish completion script")
    {
        this.SetAction(Execute);
    }

    private Task<int> Execute(ParseResult parseResult, CancellationToken ct)
    {
        var script = """
            # Surgewave CLI fish completion
            complete -c surgewave -f

            # Main commands
            complete -c surgewave -n __fish_use_subcommand -a topics -d 'Manage topics'
            complete -c surgewave -n __fish_use_subcommand -a groups -d 'Manage consumer groups'
            complete -c surgewave -n __fish_use_subcommand -a broker -d 'Broker information and health'
            complete -c surgewave -n __fish_use_subcommand -a produce -d 'Produce messages to a topic'
            complete -c surgewave -n __fish_use_subcommand -a consume -d 'Consume messages from a topic'
            complete -c surgewave -n __fish_use_subcommand -a completion -d 'Generate shell completion scripts'

            # Topics subcommands
            complete -c surgewave -n '__fish_seen_subcommand_from topics' -a list -d 'List all topics'
            complete -c surgewave -n '__fish_seen_subcommand_from topics' -a create -d 'Create a new topic'
            complete -c surgewave -n '__fish_seen_subcommand_from topics' -a delete -d 'Delete a topic'
            complete -c surgewave -n '__fish_seen_subcommand_from topics' -a describe -d 'Describe a topic'

            # Groups subcommands
            complete -c surgewave -n '__fish_seen_subcommand_from groups' -a list -d 'List all consumer groups'
            complete -c surgewave -n '__fish_seen_subcommand_from groups' -a describe -d 'Describe a consumer group'
            complete -c surgewave -n '__fish_seen_subcommand_from groups' -a delete -d 'Delete a consumer group'

            # Broker subcommands
            complete -c surgewave -n '__fish_seen_subcommand_from broker' -a info -d 'Display broker information'
            complete -c surgewave -n '__fish_seen_subcommand_from broker' -a health -d 'Check broker health'

            # Completion subcommands
            complete -c surgewave -n '__fish_seen_subcommand_from completion' -a bash -d 'Generate bash completion'
            complete -c surgewave -n '__fish_seen_subcommand_from completion' -a powershell -d 'Generate PowerShell completion'
            complete -c surgewave -n '__fish_seen_subcommand_from completion' -a zsh -d 'Generate zsh completion'
            complete -c surgewave -n '__fish_seen_subcommand_from completion' -a fish -d 'Generate fish completion'

            # Global options
            complete -c surgewave -l bootstrap-server -s b -d 'Broker address'
            complete -c surgewave -l verbose -s v -d 'Show detailed output'
            complete -c surgewave -l format -s f -a 'table json plain' -d 'Output format'
            """;

        Console.WriteLine(script);
        Console.WriteLineToError("\n# Save to ~/.config/fish/completions/surgewave.fish");
        return Task.FromResult(0);
    }
}
