namespace Kuestenlogik.Surgewave.Setup;

/// <summary>
/// Auth mechanism the operator picked in the wizard. Maps onto the
/// <c>Surgewave:Security</c> section of the generated appsettings.json
/// — <see cref="None"/> omits the section entirely so operators on a
/// trusted LAN don't carry dead config.
/// </summary>
public enum SetupAuthMethod
{
    None,
    SaslPlain,
    SaslScram,
    Tls,
    MutualTls,
}
