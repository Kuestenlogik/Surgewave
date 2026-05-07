using Kuestenlogik.Surgewave.Connect.Distributed;
using Kuestenlogik.Surgewave.Connect.Pipelines;

namespace Kuestenlogik.Surgewave.Connect.Tests.Pipelines;

public class PipelineOrchestratorDistributedTests
{
    [Fact]
    public void TaskAssignmentTracker_TrackAndRetrieveAssignment()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();

        // Act
        tracker.TrackAssignment("pipeline-p1-node1", "worker-A", "p1", "node1");

        // Assert
        var assignment = tracker.GetAssignment("pipeline-p1-node1");
        Assert.NotNull(assignment);
        Assert.Equal("pipeline-p1-node1", assignment.ConnectorName);
        Assert.Equal("worker-A", assignment.WorkerId);
        Assert.Equal("p1", assignment.PipelineId);
        Assert.Equal("node1", assignment.NodeId);
    }

    [Fact]
    public void TaskAssignmentTracker_GetAssignmentsForWorker()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");
        tracker.TrackAssignment("conn-2", "worker-A", "p1", "n2");
        tracker.TrackAssignment("conn-3", "worker-B", "p1", "n3");

        // Act
        var workerAAssignments = tracker.GetAssignmentsForWorker("worker-A");

        // Assert
        Assert.Equal(2, workerAAssignments.Count);
        Assert.All(workerAAssignments, a => Assert.Equal("worker-A", a.WorkerId));
    }

    [Fact]
    public void TaskAssignmentTracker_GetAssignmentsForPipeline()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");
        tracker.TrackAssignment("conn-2", "worker-B", "p1", "n2");
        tracker.TrackAssignment("conn-3", "worker-A", "p2", "n1");

        // Act
        var p1Assignments = tracker.GetAssignmentsForPipeline("p1");

        // Assert
        Assert.Equal(2, p1Assignments.Count);
        Assert.All(p1Assignments, a => Assert.Equal("p1", a.PipelineId));
    }

    [Fact]
    public void TaskAssignmentTracker_RemoveAssignment()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");

        // Act
        var removed = tracker.RemoveAssignment("conn-1");

        // Assert
        Assert.True(removed);
        Assert.Null(tracker.GetAssignment("conn-1"));
    }

    [Fact]
    public void TaskAssignmentTracker_RemoveWorkerAssignments_ReturnsOrphanedTasks()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");
        tracker.TrackAssignment("conn-2", "worker-A", "p1", "n2");
        tracker.TrackAssignment("conn-3", "worker-B", "p1", "n3");

        // Act
        var orphaned = tracker.RemoveWorkerAssignments("worker-A");

        // Assert
        Assert.Equal(2, orphaned.Count);
        Assert.All(orphaned, a => Assert.Equal("worker-A", a.WorkerId));
        Assert.Null(tracker.GetAssignment("conn-1"));
        Assert.Null(tracker.GetAssignment("conn-2"));
        Assert.NotNull(tracker.GetAssignment("conn-3")); // worker-B still there
    }

    [Fact]
    public void TaskAssignmentTracker_GetOwningWorker_ReturnsNull_WhenNotAssigned()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();

        // Act
        var owner = tracker.GetOwningWorker("nonexistent");

        // Assert
        Assert.Null(owner);
    }

    [Fact]
    public void TaskAssignmentTracker_GetAllAssignments()
    {
        // Arrange
        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");
        tracker.TrackAssignment("conn-2", "worker-B", "p2", "n2");

        // Act
        var all = tracker.GetAllAssignments();

        // Assert
        Assert.Equal(2, all.Count);
    }

    [Fact]
    public void RemoteTaskAssignment_SerializesCorrectly()
    {
        // Arrange
        var assignment = new RemoteTaskAssignment
        {
            ConnectorName = "pipeline-p1-n1",
            ConnectorType = "Kuestenlogik.Test.MySource",
            WorkerId = "worker-A",
            Config = new Dictionary<string, string>
            {
                ["connector.class"] = "Kuestenlogik.Test.MySource",
                ["name"] = "pipeline-p1-n1",
                ["pipeline.id"] = "p1",
                ["node.id"] = "n1",
                ["host"] = "localhost"
            },
            PipelineId = "p1",
            NodeId = "n1",
            Timestamp = 1710500000000
        };

        // Act
        var json = System.Text.Json.JsonSerializer.Serialize(assignment);
        var deserialized = System.Text.Json.JsonSerializer.Deserialize<RemoteTaskAssignment>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal("pipeline-p1-n1", deserialized.ConnectorName);
        Assert.Equal("Kuestenlogik.Test.MySource", deserialized.ConnectorType);
        Assert.Equal("worker-A", deserialized.WorkerId);
        Assert.Equal("p1", deserialized.PipelineId);
        Assert.Equal("n1", deserialized.NodeId);
        Assert.Equal(5, deserialized.Config.Count);
    }

    [Fact]
    public void RemoteTaskCommand_SerializesCorrectly()
    {
        // Arrange
        var command = new RemoteTaskCommand
        {
            ConnectorName = "pipeline-p1-n1",
            WorkerId = "worker-A",
            Command = "stop",
            Timestamp = 1710500000000
        };

        // Act
        var json = System.Text.Json.JsonSerializer.Serialize(command);
        var deserialized = System.Text.Json.JsonSerializer.Deserialize<RemoteTaskCommand>(json);

        // Assert
        Assert.NotNull(deserialized);
        Assert.Equal("pipeline-p1-n1", deserialized.ConnectorName);
        Assert.Equal("worker-A", deserialized.WorkerId);
        Assert.Equal("stop", deserialized.Command);
    }

    [Fact]
    public void AggregatedRegistry_WorkerDisconnect_CleansUpAndAllowsReassignment()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        registry.UpdateFromHeartbeat("worker-A", [
            new ConnectorCapability("Kuestenlogik.Test.Source", "source", "Test Source", "1.0.0")
        ]);
        registry.UpdateFromHeartbeat("worker-B", [
            new ConnectorCapability("Kuestenlogik.Test.Source", "source", "Test Source", "1.0.0")
        ]);

        var tracker = new TaskAssignmentTracker();
        tracker.TrackAssignment("conn-1", "worker-A", "p1", "n1");

        // Act — simulate worker-A disconnect
        registry.RemoveWorker("worker-A");
        var orphaned = tracker.RemoveWorkerAssignments("worker-A");

        // Assert
        Assert.Single(orphaned);
        var workers = registry.GetWorkersForType("Kuestenlogik.Test.Source");
        Assert.Single(workers);
        Assert.Contains("worker-B", workers);

        // Reassign to worker-B
        foreach (var assignment in orphaned)
        {
            tracker.TrackAssignment(assignment.ConnectorName, "worker-B", assignment.PipelineId, assignment.NodeId);
        }

        var newAssignment = tracker.GetAssignment("conn-1");
        Assert.NotNull(newAssignment);
        Assert.Equal("worker-B", newAssignment.WorkerId);
    }

    [Fact]
    public void AggregatedRegistry_GetWorkersForType_ReturnsEmpty_WhenNoWorkers()
    {
        // Arrange
        var registry = new AggregatedConnectorRegistry();

        // Act
        var workers = registry.GetWorkersForType("Kuestenlogik.Nonexistent.Type");

        // Assert
        Assert.Empty(workers);
    }

    [Fact]
    public void NodeStatus_IncludesWorkerId_ForRemoteNodes()
    {
        // Arrange
        var status = new NodeStatus
        {
            NodeId = "n1",
            ConnectorName = "pipeline-p1-n1",
            State = "Running",
            TaskCount = 1,
            WorkerId = "worker-A"
        };

        // Assert
        Assert.Equal("worker-A", status.WorkerId);
    }

    [Fact]
    public void NodeStatus_WorkerIdIsNull_ForLocalNodes()
    {
        // Arrange
        var status = new NodeStatus
        {
            NodeId = "n1",
            ConnectorName = "pipeline-p1-n1",
            State = "Running",
            TaskCount = 1
        };

        // Assert
        Assert.Null(status.WorkerId);
    }

    [Fact]
    public void PipelineRunState_TracksRemoteNodes()
    {
        // Arrange
        var runState = new PipelineRunState();

        // Act
        runState.NodeConnectors["n1"] = "pipeline-p1-n1";
        runState.NodeConnectors["n2"] = "pipeline-p1-n2";
        runState.RemoteNodes["n2"] = "worker-B";

        // Assert
        Assert.Equal(2, runState.NodeConnectors.Count);
        Assert.Single(runState.RemoteNodes);
        Assert.True(runState.RemoteNodes.ContainsKey("n2"));
        Assert.Equal("worker-B", runState.RemoteNodes["n2"]);
    }

    [Fact]
    public void WorkerDisconnectedEventArgs_ContainsWorkerInfo()
    {
        // Arrange & Act
        var args = new WorkerDisconnectedEventArgs("worker-A", "http://worker-a:8083");

        // Assert
        Assert.Equal("worker-A", args.WorkerId);
        Assert.Equal("http://worker-a:8083", args.RestUrl);
    }
}
