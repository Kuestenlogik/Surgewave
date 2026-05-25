namespace Kuestenlogik.Surgewave.Streams.Sql;

/// <summary>
/// Recursive-descent parser for streaming SQL.
/// Supports SELECT, CREATE STREAM AS SELECT, CREATE TABLE AS SELECT, INSERT INTO SELECT.
/// </summary>
internal sealed class SqlParser
{
    private readonly List<SqlToken> _tokens;
    private int _pos;

    public SqlParser(List<SqlToken> tokens)
    {
        _tokens = tokens;
    }

    public static SqlStatement Parse(string sql)
    {
        var lexer = new SqlLexer(sql);
        var tokens = lexer.Tokenize();
        var parser = new SqlParser(tokens);
        return parser.ParseStatement();
    }

    private static double ParseNumber(string text) =>
        double.Parse(text, System.Globalization.CultureInfo.InvariantCulture);

    private SqlToken Current => _tokens[_pos];
    private SqlToken Peek(int offset = 1) =>
        _pos + offset < _tokens.Count ? _tokens[_pos + offset] : new SqlToken(SqlTokenType.Eof, "", -1);

    private SqlToken Advance()
    {
        var token = Current;
        _pos++;
        return token;
    }

    private SqlToken Expect(SqlTokenType type)
    {
        if (Current.Type != type)
            throw new SqlParseException($"Expected {type}, got {Current.Type} '{Current.Value}' at position {Current.Position}");
        return Advance();
    }

    private bool Match(SqlTokenType type)
    {
        if (Current.Type != type) return false;
        _pos++;
        return true;
    }

    private bool IsIdentifier(string value) =>
        Current.Type == SqlTokenType.Identifier &&
        Current.Value.Equals(value, StringComparison.OrdinalIgnoreCase);

    // === Statement parsing ===

    private SqlStatement ParseStatement()
    {
        return Current.Type switch
        {
            SqlTokenType.Select => ParseSelectStatement(),
            SqlTokenType.Create => ParseCreateStatement(),
            SqlTokenType.Drop => ParseDropStatement(),
            SqlTokenType.Insert => ParseInsertStatement(),
            _ => throw new SqlParseException($"Unexpected token {Current.Type} '{Current.Value}' at position {Current.Position}")
        };
    }

    private SelectStatement ParseSelectStatement()
    {
        Expect(SqlTokenType.Select);

        var columns = ParseSelectItems();

        Expect(SqlTokenType.From);
        var source = ParseSource();

        SqlExpression? where = null;
        if (Match(SqlTokenType.Where))
            where = ParseExpression();

        IReadOnlyList<SqlExpression>? groupBy = null;
        if (Current.Type == SqlTokenType.GroupBy)
        {
            Advance(); // GROUP
            if (IsIdentifier("BY")) Advance(); // BY
            groupBy = ParseExpressionList();
        }

        SqlExpression? having = null;
        if (Match(SqlTokenType.Having))
            having = ParseExpression();

        IReadOnlyList<OrderByItem>? orderBy = null;
        if (Current.Type == SqlTokenType.OrderBy)
        {
            Advance(); // ORDER
            if (IsIdentifier("BY")) Advance(); // BY
            orderBy = ParseOrderByList();
        }

        int? limit = null;
        if (Match(SqlTokenType.Limit))
        {
            var num = Expect(SqlTokenType.NumberLiteral);
            limit = (int)ParseNumber(num.Value);
        }

        var emitChanges = false;
        if (Match(SqlTokenType.Emit))
        {
            Expect(SqlTokenType.Changes);
            emitChanges = true;
        }

        Match(SqlTokenType.Semicolon);

        return new SelectStatement(columns, source, where, groupBy, having, orderBy, limit, emitChanges);
    }

    private SqlStatement ParseCreateStatement()
    {
        Expect(SqlTokenType.Create);

        // CREATE MATERIALIZED VIEW [IF NOT EXISTS] name AS SELECT ...
        if (Current.Type == SqlTokenType.Materialized)
        {
            Advance(); // MATERIALIZED
            Expect(SqlTokenType.View);

            var ifNotExists = false;
            if (IsIdentifier("IF"))
            {
                Advance();
                if (Current.Type != SqlTokenType.Not)
                    throw new SqlParseException($"Expected NOT after IF at position {Current.Position}");
                Advance();
                if (!IsIdentifier("EXISTS"))
                    throw new SqlParseException($"Expected EXISTS after IF NOT at position {Current.Position}");
                Advance();
                ifNotExists = true;
            }

            var viewName = Expect(SqlTokenType.Identifier).Value;

            Dictionary<string, string>? viewProps = null;
            if (Current.Type == SqlTokenType.With)
                viewProps = ParseWithProperties();

            Expect(SqlTokenType.As);
            var viewQuery = ParseSelectStatement();
            return new CreateMaterializedViewAsSelect(viewName, viewQuery, viewProps, ifNotExists);
        }

        var isTable = Current.Type == SqlTokenType.Table;
        if (!isTable && Current.Type != SqlTokenType.Stream)
            throw new SqlParseException($"Expected STREAM, TABLE or MATERIALIZED VIEW after CREATE at position {Current.Position}");
        Advance();

        var name = Expect(SqlTokenType.Identifier).Value;

        // Optional WITH properties
        Dictionary<string, string>? withProps = null;
        if (Current.Type == SqlTokenType.With)
        {
            withProps = ParseWithProperties();
        }

        Expect(SqlTokenType.As);
        var query = ParseSelectStatement();

        return isTable
            ? new CreateTableAsSelect(name, query, withProps)
            : new CreateStreamAsSelect(name, query, withProps);
    }

    private SqlStatement ParseDropStatement()
    {
        Expect(SqlTokenType.Drop);

        // DROP MATERIALIZED VIEW [IF EXISTS] name
        if (Current.Type == SqlTokenType.Materialized)
        {
            Advance();
            Expect(SqlTokenType.View);
        }
        else if (Current.Type == SqlTokenType.View)
        {
            // Treat plain VIEW as materialized view
            Advance();
        }
        else
        {
            throw new SqlParseException($"Expected MATERIALIZED VIEW or VIEW after DROP at position {Current.Position}");
        }

        var ifExists = false;
        if (IsIdentifier("IF"))
        {
            Advance();
            if (!IsIdentifier("EXISTS"))
                throw new SqlParseException($"Expected EXISTS after IF at position {Current.Position}");
            Advance();
            ifExists = true;
        }

        var name = Expect(SqlTokenType.Identifier).Value;
        Match(SqlTokenType.Semicolon);
        return new DropMaterializedView(name, ifExists);
    }

    private InsertIntoSelect ParseInsertStatement()
    {
        Expect(SqlTokenType.Insert);
        Expect(SqlTokenType.Into);
        var target = Expect(SqlTokenType.Identifier).Value;
        var query = ParseSelectStatement();
        return new InsertIntoSelect(target, query);
    }

    private Dictionary<string, string> ParseWithProperties()
    {
        Expect(SqlTokenType.With);
        Expect(SqlTokenType.LeftParen);

        var props = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        while (Current.Type != SqlTokenType.RightParen)
        {
            var key = Expect(SqlTokenType.Identifier).Value;
            Expect(SqlTokenType.Equals);
            var value = Current.Type == SqlTokenType.StringLiteral
                ? Advance().Value
                : Expect(SqlTokenType.Identifier).Value;
            props[key] = value;
            if (!Match(SqlTokenType.Comma)) break;
        }
        Expect(SqlTokenType.RightParen);
        return props;
    }

    // === SELECT items ===

    private List<SelectItem> ParseSelectItems()
    {
        var items = new List<SelectItem>();
        do
        {
            if (Current.Type == SqlTokenType.Star)
            {
                Advance();
                items.Add(new StarSelectItem());
            }
            else
            {
                var expr = ParseExpression();
                string? alias = null;
                if (Match(SqlTokenType.As))
                    alias = Expect(SqlTokenType.Identifier).Value;
                else if (Current.Type == SqlTokenType.Identifier && !IsReservedAfterFrom())
                {
                    // Implicit alias (no AS keyword)
                    alias = Advance().Value;
                }
                items.Add(new ExpressionSelectItem(expr, alias));
            }
        } while (Match(SqlTokenType.Comma));

        return items;
    }

    // === Source parsing ===

    private SqlSource ParseSource()
    {
        var left = ParsePrimarySource();

        while (IsJoinKeyword())
        {
            var joinType = ParseJoinType();
            var right = ParsePrimarySource();

            SqlExpression? on = null;
            WindowSpec? window = null;

            if (Current.Type == SqlTokenType.Window)
            {
                window = ParseWindowSpec();
            }

            if (Match(SqlTokenType.On))
            {
                on = ParseExpression();
            }

            on ??= new LiteralBool(true);
            left = new JoinSource(left, right, joinType, on, window);
        }

        return left;
    }

    private SqlSource ParsePrimarySource()
    {
        var name = Expect(SqlTokenType.Identifier).Value;
        string? alias = null;
        if (Match(SqlTokenType.As))
            alias = Expect(SqlTokenType.Identifier).Value;
        else if (Current.Type == SqlTokenType.Identifier && !IsReservedAfterFrom())
            alias = Advance().Value;
        return new TableSource(name, alias);
    }

    private bool IsReservedAfterFrom() => Current.Type is SqlTokenType.Where or SqlTokenType.GroupBy
        or SqlTokenType.Having or SqlTokenType.OrderBy or SqlTokenType.Limit
        or SqlTokenType.Emit or SqlTokenType.Join or SqlTokenType.Left
        or SqlTokenType.Right or SqlTokenType.Full or SqlTokenType.Inner
        or SqlTokenType.On or SqlTokenType.Window;

    private bool IsJoinKeyword() => Current.Type is SqlTokenType.Join or SqlTokenType.Left
        or SqlTokenType.Right or SqlTokenType.Full or SqlTokenType.Inner;

    private JoinType ParseJoinType()
    {
        var joinType = JoinType.Inner;
        if (Match(SqlTokenType.Left)) { joinType = JoinType.Left; Match(SqlTokenType.Outer); }
        else if (Match(SqlTokenType.Right)) { joinType = JoinType.Right; Match(SqlTokenType.Outer); }
        else if (Match(SqlTokenType.Full)) { joinType = JoinType.Full; Match(SqlTokenType.Outer); }
        else if (Match(SqlTokenType.Inner)) { /* default */ }
        Expect(SqlTokenType.Join);
        return joinType;
    }

    // === Window specification ===

    private WindowSpec ParseWindowSpec()
    {
        // WINDOW TUMBLE(col, INTERVAL '5' MINUTES)
        Expect(SqlTokenType.Window);

        WindowType windowType;
        if (Current.Type == SqlTokenType.Tumble) { windowType = WindowType.Tumble; Advance(); }
        else if (Current.Type == SqlTokenType.Hop) { windowType = WindowType.Hop; Advance(); }
        else if (Current.Type == SqlTokenType.Session) { windowType = WindowType.Session; Advance(); }
        else throw new SqlParseException($"Expected TUMBLE, HOP, or SESSION at position {Current.Position}");

        Expect(SqlTokenType.LeftParen);
        var timeColumn = Expect(SqlTokenType.Identifier).Value;
        Expect(SqlTokenType.Comma);
        var size = ParseInterval();

        TimeSpan? advance = null;
        if (windowType == WindowType.Hop && Match(SqlTokenType.Comma))
        {
            advance = ParseInterval();
        }

        Expect(SqlTokenType.RightParen);
        return new WindowSpec(windowType, timeColumn, size, advance);
    }

    private TimeSpan ParseInterval()
    {
        // Accepts: INTERVAL 'N' UNIT  or just  N UNIT  or just  'N UNIT'
        if (IsIdentifier("INTERVAL")) Advance();

        double amount;
        if (Current.Type == SqlTokenType.StringLiteral)
        {
            var parts = Advance().Value.Split(' ', StringSplitOptions.RemoveEmptyEntries);
            amount = ParseNumber(parts[0]);
            if (parts.Length > 1) return ParseTimeUnit(amount, parts[1]);
        }
        else
        {
            amount = ParseNumber(Expect(SqlTokenType.NumberLiteral).Value);
        }

        var unit = Expect(SqlTokenType.Identifier).Value;
        return ParseTimeUnit(amount, unit);
    }

    private static TimeSpan ParseTimeUnit(double amount, string unit) => unit.ToUpperInvariant() switch
    {
        "MS" or "MILLISECOND" or "MILLISECONDS" => TimeSpan.FromMilliseconds(amount),
        "S" or "SECOND" or "SECONDS" => TimeSpan.FromSeconds(amount),
        "M" or "MIN" or "MINUTE" or "MINUTES" => TimeSpan.FromMinutes(amount),
        "H" or "HOUR" or "HOURS" => TimeSpan.FromHours(amount),
        "D" or "DAY" or "DAYS" => TimeSpan.FromDays(amount),
        _ => throw new SqlParseException($"Unknown time unit: {unit}")
    };

    // === Expression parsing (precedence climbing) ===

    private SqlExpression ParseExpression() => ParseOr();

    private SqlExpression ParseOr()
    {
        var left = ParseAnd();
        while (Current.Type == SqlTokenType.Or)
        {
            Advance();
            left = new BinaryOp(left, BinaryOperator.Or, ParseAnd());
        }
        return left;
    }

    private SqlExpression ParseAnd()
    {
        var left = ParseNot();
        while (Current.Type == SqlTokenType.And)
        {
            Advance();
            left = new BinaryOp(left, BinaryOperator.And, ParseNot());
        }
        return left;
    }

    private SqlExpression ParseNot()
    {
        if (Match(SqlTokenType.Not))
            return new UnaryOp(UnaryOperator.Not, ParseNot());
        return ParseComparison();
    }

    private SqlExpression ParseComparison()
    {
        var left = ParseAddSub();

        // IS [NOT] NULL
        if (Current.Type == SqlTokenType.Is)
        {
            Advance();
            var negated = Match(SqlTokenType.Not);
            Expect(SqlTokenType.Null);
            return new IsNullExpression(left, negated);
        }

        // [NOT] IN (...)
        var notNegated = false;
        if (Current.Type == SqlTokenType.Not)
        {
            notNegated = true;
            Advance();
        }

        if (Current.Type == SqlTokenType.In)
        {
            Advance();
            Expect(SqlTokenType.LeftParen);
            var values = ParseExpressionList();
            Expect(SqlTokenType.RightParen);
            return new InExpression(left, values, notNegated);
        }

        if (Current.Type == SqlTokenType.Between)
        {
            Advance();
            var low = ParseAddSub();
            Expect(SqlTokenType.And);
            var high = ParseAddSub();
            return new BetweenExpression(left, low, high, notNegated);
        }

        if (Current.Type == SqlTokenType.Like)
        {
            Advance();
            var pattern = Expect(SqlTokenType.StringLiteral).Value;
            return new LikeExpression(left, pattern, notNegated);
        }

        if (notNegated)
            throw new SqlParseException($"Expected IN, BETWEEN, or LIKE after NOT at position {Current.Position}");

        // Standard comparison operators
        var op = Current.Type switch
        {
            SqlTokenType.Equals => BinaryOperator.Equals,
            SqlTokenType.NotEquals => BinaryOperator.NotEquals,
            SqlTokenType.LessThan => BinaryOperator.LessThan,
            SqlTokenType.GreaterThan => BinaryOperator.GreaterThan,
            SqlTokenType.LessEqual => BinaryOperator.LessEqual,
            SqlTokenType.GreaterEqual => BinaryOperator.GreaterEqual,
            _ => (BinaryOperator?)null
        };

        if (op.HasValue)
        {
            Advance();
            return new BinaryOp(left, op.Value, ParseAddSub());
        }

        return left;
    }

    private SqlExpression ParseAddSub()
    {
        var left = ParseMulDiv();
        while (Current.Type is SqlTokenType.Plus or SqlTokenType.Minus)
        {
            var op = Current.Type == SqlTokenType.Plus ? BinaryOperator.Plus : BinaryOperator.Minus;
            Advance();
            left = new BinaryOp(left, op, ParseMulDiv());
        }
        return left;
    }

    private SqlExpression ParseMulDiv()
    {
        var left = ParseUnary();
        while (Current.Type is SqlTokenType.Star or SqlTokenType.Slash or SqlTokenType.Percent)
        {
            var op = Current.Type switch
            {
                SqlTokenType.Star => BinaryOperator.Multiply,
                SqlTokenType.Slash => BinaryOperator.Divide,
                _ => BinaryOperator.Modulo
            };
            Advance();
            left = new BinaryOp(left, op, ParseUnary());
        }
        return left;
    }

    private SqlExpression ParseUnary()
    {
        if (Current.Type == SqlTokenType.Minus)
        {
            Advance();
            return new UnaryOp(UnaryOperator.Minus, ParsePrimary());
        }
        return ParsePrimary();
    }

    private SqlExpression ParsePrimary()
    {
        switch (Current.Type)
        {
            case SqlTokenType.NumberLiteral:
                return new LiteralNumber(ParseNumber(Advance().Value));

            case SqlTokenType.StringLiteral:
                return new LiteralString(Advance().Value);

            case SqlTokenType.BoolLiteral:
                return new LiteralBool(Advance().Value.Equals("TRUE", StringComparison.OrdinalIgnoreCase));

            case SqlTokenType.Null:
                Advance();
                return new LiteralNull();

            case SqlTokenType.LeftParen:
                Advance();
                var expr = ParseExpression();
                Expect(SqlTokenType.RightParen);
                return expr;

            case SqlTokenType.Case:
                return ParseCaseExpression();

            case SqlTokenType.Cast:
                return ParseCastExpression();

            case SqlTokenType.Identifier:
            // Aggregate function keywords that look like identifiers
            case SqlTokenType.Left: // LEFT() function
            case SqlTokenType.Right: // RIGHT() function
                return ParseIdentifierOrFunction();

            default:
                throw new SqlParseException(
                    $"Unexpected token {Current.Type} '{Current.Value}' at position {Current.Position}");
        }
    }

    private SqlExpression ParseIdentifierOrFunction()
    {
        var name = Advance().Value;

        // Function call: NAME(...)
        if (Current.Type == SqlTokenType.LeftParen)
        {
            Advance();
            var distinct = Match(SqlTokenType.Distinct);
            List<SqlExpression> args;
            if (Current.Type == SqlTokenType.RightParen)
                args = [];
            else if (Current.Type == SqlTokenType.Star)
            {
                Advance(); // COUNT(*) — star as argument placeholder
                args = [];
            }
            else
                args = ParseExpressionList();
            Expect(SqlTokenType.RightParen);
            return new FunctionCall(name.ToUpperInvariant(), args, distinct);
        }

        // Qualified column: TABLE.COLUMN
        if (Current.Type == SqlTokenType.Dot)
        {
            Advance();
            var col = Advance().Value;
            return new ColumnRef(name, col);
        }

        return new ColumnRef(null, name);
    }

    private CaseExpression ParseCaseExpression()
    {
        Expect(SqlTokenType.Case);
        SqlExpression? operand = null;
        if (Current.Type != SqlTokenType.When)
            operand = ParseExpression();

        var whens = new List<WhenClause>();
        while (Match(SqlTokenType.When))
        {
            var condition = ParseExpression();
            Expect(SqlTokenType.Then);
            var result = ParseExpression();
            whens.Add(new WhenClause(condition, result));
        }

        SqlExpression? elseResult = null;
        if (Match(SqlTokenType.Else))
            elseResult = ParseExpression();

        Expect(SqlTokenType.End);
        return new CaseExpression(operand, whens, elseResult);
    }

    private CastExpression ParseCastExpression()
    {
        Expect(SqlTokenType.Cast);
        Expect(SqlTokenType.LeftParen);
        var expr = ParseExpression();
        Expect(SqlTokenType.As);
        var typeName = Expect(SqlTokenType.Identifier).Value;
        Expect(SqlTokenType.RightParen);
        return new CastExpression(expr, typeName);
    }

    // === Helpers ===

    private List<SqlExpression> ParseExpressionList()
    {
        var list = new List<SqlExpression> { ParseExpression() };
        while (Match(SqlTokenType.Comma))
            list.Add(ParseExpression());
        return list;
    }

    private List<OrderByItem> ParseOrderByList()
    {
        var list = new List<OrderByItem>();
        do
        {
            var expr = ParseExpression();
            var desc = false;
            if (Match(SqlTokenType.Desc)) desc = true;
            else Match(SqlTokenType.Asc);
            list.Add(new OrderByItem(expr, desc));
        } while (Match(SqlTokenType.Comma));
        return list;
    }
}
