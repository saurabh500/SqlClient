using System;
using System.Buffers;

namespace Microsoft.Data.SqlClientX.Handlers.Connection
{
    internal class TdsPacketSegment : ReadOnlySequenceSegment<byte>
    {
        public TdsPacketSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        public TdsPacketSegment Append(ReadOnlyMemory<byte> memory)
        {
            var segment = new TdsPacketSegment(memory)
            {
                RunningIndex = RunningIndex + Memory.Length
            };

            Next = segment;

            return segment;
        }
    }
}
