using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for SurgewaveOpCode enum definitions and groupings
/// </summary>
public sealed class OpCodeTests
{
    [Fact]
    public void OpCode_None_IsZero()
    {
        Assert.Equal(0x0000, (ushort)SurgewaveOpCode.None);
    }

    [Fact]
    public void OpCode_ConnectionGroup_HasCorrectValues()
    {
        Assert.Equal(0x0001, (ushort)SurgewaveOpCode.Handshake);
        Assert.Equal(0x0002, (ushort)SurgewaveOpCode.Ping);
        Assert.Equal(0x0003, (ushort)SurgewaveOpCode.Pong);
        Assert.Equal(0x0004, (ushort)SurgewaveOpCode.GetMetadata);
    }

    [Fact]
    public void OpCode_ProduceGroup_HasCorrectValues()
    {
        Assert.Equal(0x0100, (ushort)SurgewaveOpCode.Produce);
        Assert.Equal(0x0101, (ushort)SurgewaveOpCode.ProduceBatch);
        Assert.Equal(0x0102, (ushort)SurgewaveOpCode.ProduceAck);
    }

    [Fact]
    public void OpCode_ConsumeGroup_HasCorrectValues()
    {
        Assert.Equal(0x0200, (ushort)SurgewaveOpCode.Fetch);
        Assert.Equal(0x0201, (ushort)SurgewaveOpCode.FetchResponse);
        Assert.Equal(0x0202, (ushort)SurgewaveOpCode.Subscribe);
        Assert.Equal(0x0203, (ushort)SurgewaveOpCode.Unsubscribe);
        Assert.Equal(0x0204, (ushort)SurgewaveOpCode.Nack);
        Assert.Equal(0x0205, (ushort)SurgewaveOpCode.NackAck);
    }

    [Fact]
    public void OpCode_OffsetGroup_HasCorrectValues()
    {
        Assert.Equal(0x0300, (ushort)SurgewaveOpCode.CommitOffset);
        Assert.Equal(0x0301, (ushort)SurgewaveOpCode.FetchOffset);
        Assert.Equal(0x0302, (ushort)SurgewaveOpCode.ListOffsets);
    }

    [Fact]
    public void OpCode_ConsumerGroupOps_HasCorrectValues()
    {
        Assert.Equal(0x0400, (ushort)SurgewaveOpCode.JoinGroup);
        Assert.Equal(0x0401, (ushort)SurgewaveOpCode.SyncGroup);
        Assert.Equal(0x0402, (ushort)SurgewaveOpCode.LeaveGroup);
        Assert.Equal(0x0403, (ushort)SurgewaveOpCode.Heartbeat);
        Assert.Equal(0x0404, (ushort)SurgewaveOpCode.ListGroups);
        Assert.Equal(0x0405, (ushort)SurgewaveOpCode.DescribeGroup);
        Assert.Equal(0x0406, (ushort)SurgewaveOpCode.DeleteGroup);
        Assert.Equal(0x0407, (ushort)SurgewaveOpCode.FindCoordinator);
        Assert.Equal(0x0408, (ushort)SurgewaveOpCode.GetGroupLag);
        Assert.Equal(0x0409, (ushort)SurgewaveOpCode.GetLagSummary);
    }

    [Fact]
    public void OpCode_AdminGroup_HasCorrectValues()
    {
        Assert.Equal(0x0500, (ushort)SurgewaveOpCode.CreateTopic);
        Assert.Equal(0x0501, (ushort)SurgewaveOpCode.DeleteTopic);
        Assert.Equal(0x0502, (ushort)SurgewaveOpCode.ListTopics);
        Assert.Equal(0x0503, (ushort)SurgewaveOpCode.DescribeTopic);
        Assert.Equal(0x0504, (ushort)SurgewaveOpCode.AlterConfig);
        Assert.Equal(0x0505, (ushort)SurgewaveOpCode.DescribeConfig);
        Assert.Equal(0x0506, (ushort)SurgewaveOpCode.GetClusterInfo);
        Assert.Equal(0x0507, (ushort)SurgewaveOpCode.ListBrokers);
    }

    [Fact]
    public void OpCode_TransactionGroup_HasCorrectValues()
    {
        Assert.Equal(0x0600, (ushort)SurgewaveOpCode.InitProducerId);
        Assert.Equal(0x0601, (ushort)SurgewaveOpCode.AddPartitionsToTxn);
        Assert.Equal(0x0602, (ushort)SurgewaveOpCode.AddOffsetsToTxn);
        Assert.Equal(0x0603, (ushort)SurgewaveOpCode.TxnOffsetCommit);
        Assert.Equal(0x0604, (ushort)SurgewaveOpCode.EndTxn);
        Assert.Equal(0x0605, (ushort)SurgewaveOpCode.ListTransactions);
        Assert.Equal(0x0606, (ushort)SurgewaveOpCode.DescribeTransactions);
    }

    [Fact]
    public void OpCode_SecurityGroup_HasCorrectValues()
    {
        Assert.Equal(0x0800, (ushort)SurgewaveOpCode.DescribeAcls);
        Assert.Equal(0x0801, (ushort)SurgewaveOpCode.CreateAcls);
        Assert.Equal(0x0802, (ushort)SurgewaveOpCode.DeleteAcls);
    }

    [Fact]
    public void OpCode_SchemaRegistryGroup_HasCorrectValues()
    {
        Assert.Equal(0x0A00, (ushort)SurgewaveOpCode.ListSubjects);
        Assert.Equal(0x0A01, (ushort)SurgewaveOpCode.GetSubjectVersions);
        Assert.Equal(0x0A02, (ushort)SurgewaveOpCode.RegisterSchema);
        Assert.Equal(0x0A03, (ushort)SurgewaveOpCode.GetSchemaById);
        Assert.Equal(0x0A07, (ushort)SurgewaveOpCode.CheckCompatibility);
    }

    [Fact]
    public void OpCode_ConnectGroup_HasCorrectValues()
    {
        Assert.Equal(0x0B00, (ushort)SurgewaveOpCode.ListConnectors);
        Assert.Equal(0x0B01, (ushort)SurgewaveOpCode.GetConnector);
        Assert.Equal(0x0B02, (ushort)SurgewaveOpCode.CreateConnector);
        Assert.Equal(0x0B03, (ushort)SurgewaveOpCode.DeleteConnector);
        Assert.Equal(0x0B0C, (ushort)SurgewaveOpCode.ListConnectorPlugins);
    }

    [Fact]
    public void OpCode_CrossTopicTxnGroup_HasCorrectValues()
    {
        Assert.Equal(0x0E00, (ushort)SurgewaveOpCode.CrossTopicTxnBegin);
        Assert.Equal(0x0E01, (ushort)SurgewaveOpCode.CrossTopicTxnBeginAck);
        Assert.Equal(0x0E04, (ushort)SurgewaveOpCode.CrossTopicTxnCommit);
        Assert.Equal(0x0E06, (ushort)SurgewaveOpCode.CrossTopicTxnAbort);
        Assert.Equal(0x0E07, (ushort)SurgewaveOpCode.CrossTopicTxnAbortAck);
    }

    [Fact]
    public void OpCode_Error_IsAtMaxRange()
    {
        Assert.Equal(0xFF00, (ushort)SurgewaveOpCode.Error);
    }

    [Fact]
    public void OpCode_AllValuesAreUnique()
    {
        var values = Enum.GetValues<SurgewaveOpCode>();
        var distinct = values.Select(v => (ushort)v).Distinct().ToArray();
        Assert.Equal(values.Length, distinct.Length);
    }

    [Fact]
    public void OpCode_CanRoundTripThroughUshort()
    {
        foreach (var opCode in Enum.GetValues<SurgewaveOpCode>())
        {
            var numeric = (ushort)opCode;
            var restored = (SurgewaveOpCode)numeric;
            Assert.Equal(opCode, restored);
        }
    }
}
