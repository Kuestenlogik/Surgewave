namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Tokenizer for streaming SQL syntax.
/// Supports ksqlDB-like SQL with streaming extensions (TUMBLE, HOP, EMIT CHANGES, etc.).
/// </summary>
internal sealed class SqlLexer
{
    private static readonly Dictionary<string, SqlTokenType> Keywords = new(StringComparer.OrdinalIgnoreCase)
    {
        ["SELECT"] = SqlTokenType.Select,
        ["FROM"] = SqlTokenType.From,
        ["WHERE"] = SqlTokenType.Where,
        ["GROUP"] = SqlTokenType.GroupBy, // GROUP BY handled at parser level
        ["HAVING"] = SqlTokenType.Having,
        ["ORDER"] = SqlTokenType.OrderBy, // ORDER BY handled at parser level
        ["LIMIT"] = SqlTokenType.Limit,
        ["AS"] = SqlTokenType.As,
        ["AND"] = SqlTokenType.And,
        ["OR"] = SqlTokenType.Or,
        ["NOT"] = SqlTokenType.Not,
        ["IN"] = SqlTokenType.In,
        ["BETWEEN"] = SqlTokenType.Between,
        ["LIKE"] = SqlTokenType.Like,
        ["IS"] = SqlTokenType.Is,
        ["NULL"] = SqlTokenType.Null,
        ["TRUE"] = SqlTokenType.True,
        ["FALSE"] = SqlTokenType.False,
        ["ASC"] = SqlTokenType.Asc,
        ["DESC"] = SqlTokenType.Desc,
        ["CREATE"] = SqlTokenType.Create,
        ["DROP"] = SqlTokenType.Drop,
        ["STREAM"] = SqlTokenType.Stream,
        ["TABLE"] = SqlTokenType.Table,
        ["VIEW"] = SqlTokenType.View,
        ["MATERIALIZED"] = SqlTokenType.Materialized,
        ["WITH"] = SqlTokenType.With,
        ["EMIT"] = SqlTokenType.Emit,
        ["CHANGES"] = SqlTokenType.Changes,
        ["INSERT"] = SqlTokenType.Insert,
        ["INTO"] = SqlTokenType.Into,
        ["VALUES"] = SqlTokenType.Values,
        ["DELETE"] = SqlTokenType.Delete,
        ["WINDOW"] = SqlTokenType.Window,
        ["TUMBLE"] = SqlTokenType.Tumble,
        ["HOP"] = SqlTokenType.Hop,
        ["SESSION"] = SqlTokenType.Session,
        ["JOIN"] = SqlTokenType.Join,
        ["LEFT"] = SqlTokenType.Left,
        ["RIGHT"] = SqlTokenType.Right,
        ["FULL"] = SqlTokenType.Full,
        ["INNER"] = SqlTokenType.Inner,
        ["OUTER"] = SqlTokenType.Outer,
        ["ON"] = SqlTokenType.On,
        ["CASE"] = SqlTokenType.Case,
        ["WHEN"] = SqlTokenType.When,
        ["THEN"] = SqlTokenType.Then,
        ["ELSE"] = SqlTokenType.Else,
        ["END"] = SqlTokenType.End,
        ["CAST"] = SqlTokenType.Cast,
        ["DISTINCT"] = SqlTokenType.Distinct,
        ["BY"] = SqlTokenType.Identifier, // BY is context-dependent, treated as identifier
    };

    private readonly string _input;
    private int _pos;

    public SqlLexer(string input)
    {
        _input = input;
    }

    public List<SqlToken> Tokenize()
    {
        var tokens = new List<SqlToken>();
        while (_pos < _input.Length)
        {
            SkipWhitespace();
            if (_pos >= _input.Length) break;

            var c = _input[_pos];

            // Skip line comments
            if (c == '-' && _pos + 1 < _input.Length && _input[_pos + 1] == '-')
            {
                while (_pos < _input.Length && _input[_pos] != '\n') _pos++;
                continue;
            }

            // Skip block comments
            if (c == '/' && _pos + 1 < _input.Length && _input[_pos + 1] == '*')
            {
                _pos += 2;
                while (_pos + 1 < _input.Length && !(_input[_pos] == '*' && _input[_pos + 1] == '/')) _pos++;
                _pos += 2;
                continue;
            }

            var token = c switch
            {
                '*' => Single(SqlTokenType.Star),
                ',' => Single(SqlTokenType.Comma),
                '.' => Single(SqlTokenType.Dot),
                '(' => Single(SqlTokenType.LeftParen),
                ')' => Single(SqlTokenType.RightParen),
                '+' => Single(SqlTokenType.Plus),
                '-' => Single(SqlTokenType.Minus),
                '/' => Single(SqlTokenType.Slash),
                '%' => Single(SqlTokenType.Percent),
                ';' => Single(SqlTokenType.Semicolon),
                '=' => Single(SqlTokenType.Equals),
                '<' => ReadLessOperator(),
                '>' => ReadGreaterOperator(),
                '!' => ReadNotOperator(),
                '\'' => ReadStringLiteral(),
                '`' or '"' => ReadQuotedIdentifier(c),
                _ when char.IsDigit(c) => ReadNumber(),
                _ when char.IsLetter(c) || c == '_' => ReadIdentifierOrKeyword(),
                _ => throw new SqlParseException($"Unexpected character '{c}' at position {_pos}")
            };

            tokens.Add(token);
        }

        tokens.Add(new SqlToken(SqlTokenType.Eof, "", _pos));
        return tokens;
    }

    private void SkipWhitespace()
    {
        while (_pos < _input.Length && char.IsWhiteSpace(_input[_pos])) _pos++;
    }

    private SqlToken Single(SqlTokenType type)
    {
        var token = new SqlToken(type, _input[_pos].ToString(), _pos);
        _pos++;
        return token;
    }

    private SqlToken ReadLessOperator()
    {
        var start = _pos++;
        if (_pos < _input.Length)
        {
            if (_input[_pos] == '=') { _pos++; return new SqlToken(SqlTokenType.LessEqual, "<=", start); }
            if (_input[_pos] == '>') { _pos++; return new SqlToken(SqlTokenType.NotEquals, "<>", start); }
        }
        return new SqlToken(SqlTokenType.LessThan, "<", start);
    }

    private SqlToken ReadGreaterOperator()
    {
        var start = _pos++;
        if (_pos < _input.Length && _input[_pos] == '=')
        {
            _pos++;
            return new SqlToken(SqlTokenType.GreaterEqual, ">=", start);
        }
        return new SqlToken(SqlTokenType.GreaterThan, ">", start);
    }

    private SqlToken ReadNotOperator()
    {
        var start = _pos++;
        if (_pos < _input.Length && _input[_pos] == '=')
        {
            _pos++;
            return new SqlToken(SqlTokenType.NotEquals, "!=", start);
        }
        throw new SqlParseException($"Expected '=' after '!' at position {start}");
    }

    private SqlToken ReadStringLiteral()
    {
        var start = _pos++;
        var sb = new System.Text.StringBuilder();
        while (_pos < _input.Length && _input[_pos] != '\'')
        {
            if (_input[_pos] == '\\' && _pos + 1 < _input.Length)
            {
                _pos++;
                sb.Append(_input[_pos]);
            }
            else
            {
                sb.Append(_input[_pos]);
            }
            _pos++;
        }
        if (_pos >= _input.Length) throw new SqlParseException($"Unterminated string at position {start}");
        _pos++; // skip closing quote
        return new SqlToken(SqlTokenType.StringLiteral, sb.ToString(), start);
    }

    private SqlToken ReadQuotedIdentifier(char quote)
    {
        var start = _pos++;
        var sb = new System.Text.StringBuilder();
        while (_pos < _input.Length && _input[_pos] != quote)
        {
            sb.Append(_input[_pos]);
            _pos++;
        }
        if (_pos >= _input.Length) throw new SqlParseException($"Unterminated identifier at position {start}");
        _pos++;
        return new SqlToken(SqlTokenType.Identifier, sb.ToString(), start);
    }

    private SqlToken ReadNumber()
    {
        var start = _pos;
        var hasDot = false;
        while (_pos < _input.Length && (char.IsDigit(_input[_pos]) || _input[_pos] == '.'))
        {
            if (_input[_pos] == '.')
            {
                if (hasDot) break;
                hasDot = true;
            }
            _pos++;
        }
        return new SqlToken(SqlTokenType.NumberLiteral, _input[start.._pos], start);
    }

    private SqlToken ReadIdentifierOrKeyword()
    {
        var start = _pos;
        while (_pos < _input.Length && (char.IsLetterOrDigit(_input[_pos]) || _input[_pos] == '_' || _input[_pos] == '-'))
            _pos++;

        var text = _input[start.._pos];

        if (Keywords.TryGetValue(text, out var keywordType))
        {
            // Special: TRUE/FALSE as boolean literals
            if (keywordType == SqlTokenType.True || keywordType == SqlTokenType.False)
                return new SqlToken(SqlTokenType.BoolLiteral, text, start);

            return new SqlToken(keywordType, text, start);
        }

        return new SqlToken(SqlTokenType.Identifier, text, start);
    }
}
