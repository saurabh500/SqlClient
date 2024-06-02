using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient.SNI;

namespace Microsoft.Data.SqlClient.SqlClientX.Streams
{
    internal class MarsStream : Stream
    {
        private readonly Stream _underLyingStream;
        private ushort _sessionId;
        private uint _sequenceNumber;
        private int _tdsPacketSize;
        private byte[] _smpBuffer;
        public const byte SMPacketIdentifier = 0x53;

        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override long Length => throw new NotImplementedException();

        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        private byte[] _header = new byte[SNISMUXHeader.HEADER_LENGTH];

        public MarsStream(Stream stream, MarsSequencer sequencer, int tdsPacketSize)
        {
            this._sessionId = sequencer.NextSessionId;
            this._underLyingStream = stream;
            this._sequenceNumber = 0;
            this._tdsPacketSize = tdsPacketSize;

            this._smpBuffer = new byte[this._tdsPacketSize + SNISMUXHeader.HEADER_LENGTH];
        }

        public async ValueTask Initialize(ushort sessionId,
            bool isAsync,
            CancellationToken ct)
        {
            _sessionId = sessionId;

            ConstructControlPacket(SNISMUXFlags.SMUX_SYN);
            if (!isAsync)
            {
                Write(Span<byte>.Empty);
            }
            else
            {
                await WriteAsync(Memory<byte>.Empty, ct).ConfigureAwait(false);
            }
        }

        private void ConstructControlPacket(SNISMUXFlags flags, int length=0)
        {
            ushort sessionId = _sessionId;
            _header[0] = SMPacketIdentifier;
            _header[1] = (byte)flags;
            _header[2] = (byte)(sessionId & 0xff); // BitConverter.GetBytes(_currentHeader.sessionId).CopyTo(headerBytes, 2);
            _header[3] = (byte)((sessionId >> 8) & 0xff);

            _header[4] = (byte)(length & 0xff); // BitConverter.GetBytes(_currentHeader.length).CopyTo(headerBytes, 4);
            _header[5] = (byte)((length >> 8) & 0xff);
            _header[6] = (byte)((length >> 16) & 0xff);
            _header[7] = (byte)((length >> 24) & 0xff);

            uint sequenceNumber = _sequenceNumber;
            _header[8] = (byte)(sequenceNumber & 0xff); // BitConverter.GetBytes(_currentHeader.sequenceNumber).CopyTo(headerBytes, 8);
            _header[9] = (byte)((sequenceNumber >> 8) & 0xff);
            _header[10] = (byte)((sequenceNumber >> 16) & 0xff);
            _header[11] = (byte)((sequenceNumber >> 24) & 0xff);

            uint highwater = 4;
            // access the highest element first to cause the largest range check in the jit, then fill in the rest of the value and carry on as normal
            _header[15] = (byte)((highwater >> 24) & 0xff);
            _header[12] = (byte)(highwater & 0xff); // BitConverter.GetBytes(_currentHeader.highwater).CopyTo(headerBytes, 12);
            _header[13] = (byte)((highwater >> 8) & 0xff);
            _header[14] = (byte)((highwater >> 16) & 0xff);
        }

        public override void Flush()
        {
            throw new NotImplementedException();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotImplementedException();
        }

        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            throw new NotImplementedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            // Set the header with the data header.
            SetupDataPacket(count);

            // Copy the header to the buffer.
            _header.AsSpan().CopyTo(_smpBuffer.AsSpan()[..16]);

            // Copy the data being sent to the output buffer.
            buffer.AsSpan().CopyTo(_smpBuffer.AsSpan()[16..]);

            _underLyingStream.Write(buffer, offset, count);
        }

        private void SetupDataPacket(int dataLength)
        {
            int bufferLength = GetSendBufferSize(dataLength);
            ConstructControlPacket(SNISMUXFlags.SMUX_DATA, bufferLength);
        }

        private static int GetSendBufferSize(int dataLength) =>  dataLength + SNISMUXHeader.HEADER_LENGTH;

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            // Set the header with the data header.
            SetupDataPacket(buffer.Length);

            // Copy the header to the buffer.
            _header.AsSpan().CopyTo(_smpBuffer.AsSpan()[..16]);

            // Copy the data being sent to the output buffer.
            buffer.CopyTo(_smpBuffer.AsMemory()[16..]);

            return _underLyingStream.WriteAsync(_smpBuffer, cancellationToken);
        }
    }
}
