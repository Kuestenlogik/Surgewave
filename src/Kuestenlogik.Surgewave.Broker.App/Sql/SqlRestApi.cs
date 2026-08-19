namespace Kuestenlogik.Surgewave.Broker.Sql;

/// <summary>
/// REST API endpoints for SQL query execution.
/// Provides both one-shot query execution and continuous query management.
/// </summary>
public static class SqlRestApi
{
    public static IEndpointRouteBuilder MapSqlRestApi(this IEndpointRouteBuilder app)
    {
        var group = app.MapGroup("/api/sql")
            .WithTags("SQL Query Engine");

        // POST /api/sql/execute — Execute a one-shot SQL query
        group.MapPost("/execute", (ExecuteSqlRequest request, SurgewaveSqlService sqlService) =>
        {
            if (string.IsNullOrWhiteSpace(request.Sql))
                return Results.BadRequest(new { error = "SQL query is required" });

            var result = sqlService.ExecuteQuery(request.Sql);

            if (result.Error != null)
                return Results.BadRequest(new { error = result.Error });

            return Results.Ok(result);
        })
        .WithName("ExecuteSql")
        .WithSummary("Execute a SQL query against Surgewave topics")
        .Produces<SqlExecuteResponse>()
        .ProducesProblem(StatusCodes.Status400BadRequest);

        // POST /api/sql/queries — Create a continuous query
        group.MapPost("/queries", (CreateQueryRequest request, SurgewaveSqlService sqlService) =>
        {
            if (string.IsNullOrWhiteSpace(request.Sql))
                return Results.BadRequest(new { error = "SQL query is required" });

            try
            {
                var info = sqlService.CreateContinuousQuery(request.Sql, request.Name);
                return Results.Ok(info);
            }
            catch (InvalidOperationException ex)
            {
                return Results.BadRequest(new { error = ex.Message });
            }
        })
        .WithName("CreateContinuousQuery")
        .WithSummary("Create a continuous SQL query")
        .Produces<ContinuousQueryInfo>()
        .ProducesProblem(StatusCodes.Status400BadRequest);

        // GET /api/sql/queries — List all continuous queries
        group.MapGet("/queries", (SurgewaveSqlService sqlService) =>
        {
            var queries = sqlService.ListContinuousQueries();
            return Results.Ok(queries);
        })
        .WithName("ListContinuousQueries")
        .WithSummary("List all running continuous SQL queries")
        .Produces<IReadOnlyList<ContinuousQueryInfo>>();

        // DELETE /api/sql/queries/{id} — Terminate a continuous query
        group.MapDelete("/queries/{id}", (string id, SurgewaveSqlService sqlService) =>
        {
            var terminated = sqlService.TerminateQuery(id);
            if (!terminated)
                return Results.NotFound(new { error = $"Query '{id}' not found" });

            return Results.Ok(new { status = "TERMINATED" });
        })
        .WithName("TerminateContinuousQuery")
        .WithSummary("Terminate a running continuous SQL query")
        .ProducesProblem(StatusCodes.Status404NotFound);

        return app;
    }
}
