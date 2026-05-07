using Kuestenlogik.Surgewave.Protocol.Native;
using Xunit;

namespace Kuestenlogik.Surgewave.Protocol.Native.Tests;

/// <summary>
/// Tests for SurgewaveErrorCode enum definitions
/// </summary>
public sealed class ErrorCodeTests
{
    [Fact]
    public void ErrorCode_None_IsZero()
    {
        Assert.Equal(0, (ushort)SurgewaveErrorCode.None);
    }

    [Fact]
    public void ErrorCode_BasicErrors_HaveCorrectValues()
    {
        Assert.Equal(1, (ushort)SurgewaveErrorCode.UnknownError);
        Assert.Equal(2, (ushort)SurgewaveErrorCode.InvalidRequest);
        Assert.Equal(3, (ushort)SurgewaveErrorCode.TopicNotFound);
        Assert.Equal(4, (ushort)SurgewaveErrorCode.PartitionNotFound);
        Assert.Equal(5, (ushort)SurgewaveErrorCode.NotLeader);
        Assert.Equal(6, (ushort)SurgewaveErrorCode.AuthenticationFailed);
        Assert.Equal(7, (ushort)SurgewaveErrorCode.AuthorizationFailed);
        Assert.Equal(8, (ushort)SurgewaveErrorCode.InvalidOffset);
        Assert.Equal(9, (ushort)SurgewaveErrorCode.MessageTooLarge);
    }

    [Fact]
    public void ErrorCode_GroupErrors_HaveCorrectValues()
    {
        Assert.Equal(10, (ushort)SurgewaveErrorCode.GroupNotFound);
        Assert.Equal(11, (ushort)SurgewaveErrorCode.RebalanceInProgress);
        Assert.Equal(12, (ushort)SurgewaveErrorCode.InvalidSession);
        Assert.Equal(13, (ushort)SurgewaveErrorCode.Timeout);
        Assert.Equal(14, (ushort)SurgewaveErrorCode.MemberIdRequired);
        Assert.Equal(15, (ushort)SurgewaveErrorCode.UnknownMemberId);
        Assert.Equal(16, (ushort)SurgewaveErrorCode.IllegalGeneration);
        Assert.Equal(17, (ushort)SurgewaveErrorCode.InconsistentGroupProtocol);
        Assert.Equal(18, (ushort)SurgewaveErrorCode.GroupNotEmpty);
        Assert.Equal(19, (ushort)SurgewaveErrorCode.GroupAuthorizationFailed);
        Assert.Equal(20, (ushort)SurgewaveErrorCode.NotCoordinator);
        Assert.Equal(21, (ushort)SurgewaveErrorCode.CoordinatorNotAvailable);
    }

    [Fact]
    public void ErrorCode_TransactionErrors_HaveCorrectValues()
    {
        Assert.Equal(30, (ushort)SurgewaveErrorCode.InvalidProducerEpoch);
        Assert.Equal(31, (ushort)SurgewaveErrorCode.UnknownProducerId);
        Assert.Equal(32, (ushort)SurgewaveErrorCode.InvalidTxnState);
        Assert.Equal(33, (ushort)SurgewaveErrorCode.TransactionAborted);
        Assert.Equal(34, (ushort)SurgewaveErrorCode.ConcurrentTransactions);
        Assert.Equal(35, (ushort)SurgewaveErrorCode.TransactionTimeout);
        Assert.Equal(36, (ushort)SurgewaveErrorCode.DuplicateSequenceNumber);
        Assert.Equal(37, (ushort)SurgewaveErrorCode.OutOfOrderSequenceNumber);
    }

    [Fact]
    public void ErrorCode_SecurityErrors_HaveCorrectValues()
    {
        Assert.Equal(40, (ushort)SurgewaveErrorCode.SecurityDisabled);
        Assert.Equal(41, (ushort)SurgewaveErrorCode.InvalidAclFilter);
        Assert.Equal(42, (ushort)SurgewaveErrorCode.AclNotFound);
    }

    [Fact]
    public void ErrorCode_ConfigErrors_HaveCorrectValues()
    {
        Assert.Equal(50, (ushort)SurgewaveErrorCode.InvalidConfig);
        Assert.Equal(51, (ushort)SurgewaveErrorCode.ConfigNotFound);
    }

    [Fact]
    public void ErrorCode_SchemaRegistryErrors_HaveCorrectValues()
    {
        Assert.Equal(70, (ushort)SurgewaveErrorCode.SchemaNotFound);
        Assert.Equal(71, (ushort)SurgewaveErrorCode.SubjectNotFound);
        Assert.Equal(72, (ushort)SurgewaveErrorCode.VersionNotFound);
        Assert.Equal(73, (ushort)SurgewaveErrorCode.IncompatibleSchema);
        Assert.Equal(74, (ushort)SurgewaveErrorCode.InvalidSchema);
        Assert.Equal(75, (ushort)SurgewaveErrorCode.SchemaRegistryDisabled);
    }

    [Fact]
    public void ErrorCode_ConnectErrors_HaveCorrectValues()
    {
        Assert.Equal(80, (ushort)SurgewaveErrorCode.ConnectorNotFound);
        Assert.Equal(81, (ushort)SurgewaveErrorCode.ConnectorAlreadyExists);
        Assert.Equal(82, (ushort)SurgewaveErrorCode.TaskNotFound);
        Assert.Equal(83, (ushort)SurgewaveErrorCode.InvalidConnectorConfig);
        Assert.Equal(84, (ushort)SurgewaveErrorCode.ConnectDisabled);
        Assert.Equal(85, (ushort)SurgewaveErrorCode.ConnectorFailed);
    }

    [Fact]
    public void ErrorCode_CrossTopicTxnErrors_HaveCorrectValues()
    {
        Assert.Equal(100, (ushort)SurgewaveErrorCode.CrossTopicTxnNotFound);
        Assert.Equal(101, (ushort)SurgewaveErrorCode.CrossTopicTxnInvalidState);
        Assert.Equal(102, (ushort)SurgewaveErrorCode.CrossTopicTxnTimedOut);
        Assert.Equal(103, (ushort)SurgewaveErrorCode.CrossTopicTxnMaxWritesExceeded);
        Assert.Equal(104, (ushort)SurgewaveErrorCode.CrossTopicTxnCommitFailed);
        Assert.Equal(105, (ushort)SurgewaveErrorCode.CrossTopicTxnDisabled);
    }

    [Fact]
    public void ErrorCode_AllValuesAreUnique()
    {
        var values = Enum.GetValues<SurgewaveErrorCode>();
        var distinct = values.Select(v => (ushort)v).Distinct().ToArray();
        Assert.Equal(values.Length, distinct.Length);
    }

    [Fact]
    public void ErrorCode_CanRoundTripThroughResponseHeader()
    {
        var buffer = new byte[SurgewaveResponseHeader.Size];
        foreach (var errorCode in Enum.GetValues<SurgewaveErrorCode>())
        {
            var header = new SurgewaveResponseHeader
            {
                Flags = SurgewaveProtocolFlags.None,
                RequestId = 1,
                OpCode = SurgewaveOpCode.Error,
                ErrorCode = errorCode,
                PayloadLength = 0
            };
            header.WriteTo(buffer);
            var parsed = SurgewaveResponseHeader.ReadFrom(buffer);
            Assert.Equal(errorCode, parsed.ErrorCode);
        }
    }
}
