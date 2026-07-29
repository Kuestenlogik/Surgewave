// Skips the CLR zero-initialization of method locals and stackalloc buffers across
// this assembly. Audited (#87): every stackalloc/Unsafe/interop site fully writes the
// bytes it later reads (or is guarded against short reads), so no code observes
// uninitialized stack memory. Ordinary locals remain protected by C# definite assignment.
[module: System.Runtime.CompilerServices.SkipLocalsInit]
