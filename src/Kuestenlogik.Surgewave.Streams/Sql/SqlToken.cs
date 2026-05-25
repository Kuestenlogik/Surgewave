namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Token types for the streaming SQL lexer.
/// </summary>
internal enum SqlTokenType
{
    // Literals
    Identifier,
    StringLiteral,
    NumberLiteral,
    BoolLiteral,

    // Keywords
    Select,
    From,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Limit,
    As,
    And,
    Or,
    Not,
    In,
    Between,
    Like,
    Is,
    Null,
    True,
    False,
    Asc,
    Desc,
    Create,
    Drop,
    Stream,
    Table,
    View,
    Materialized,
    With,
    Emit,
    Changes,
    Insert,
    Into,
    Values,
    Delete,
    Window,
    Tumble,
    Hop,
    Session,
    Join,
    Left,
    Right,
    Full,
    Inner,
    Outer,
    On,
    Case,
    When,
    Then,
    Else,
    End,
    Cast,
    Distinct,

    // Operators
    Star,        // *
    Comma,       // ,
    Dot,         // .
    LeftParen,   // (
    RightParen,  // )
    Equals,      // =
    NotEquals,   // != or <>
    LessThan,    // <
    GreaterThan, // >
    LessEqual,   // <=
    GreaterEqual,// >=
    Plus,        // +
    Minus,       // -
    Slash,       // /
    Percent,     // %
    Semicolon,   // ;

    // Special
    Eof
}

/// <summary>
/// A single token from the SQL lexer.
/// </summary>
internal readonly record struct SqlToken(SqlTokenType Type, string Value, int Position);
