namespace Kuestenlogik.Surgewave.Plugins.Tests.Packaging;

/// <summary>
/// Serialises the tests that mutate SurgewaveDataRoot's process-wide state, so a
/// parallel test cannot observe another's environment variables.
/// </summary>
[CollectionDefinition("SurgewaveDataRoot", DisableParallelization = true)]
public sealed class SurgewaveDataRootCollection;
