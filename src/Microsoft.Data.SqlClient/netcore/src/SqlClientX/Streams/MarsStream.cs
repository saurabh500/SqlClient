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
        //private bool _isInitialized;

        public override bool CanRead => throw new NotImplementedException();

        public override bool CanSeek => throw new NotImplementedException();

        public override bool CanWrite => throw new NotImplementedException();

        public override long Length => throw new NotImplementedException();

        public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

        private byte[] _header = new byte[SNISMUXHeader.HEADER_LENGTH];

        public MarsStream(Stream stream)
        {
            this._underLyingStream = stream;
        }

        public async ValueTask Initialize(ushort sessionId, 
            bool isAsync, 
            CancellationToken ct)
        {
            _sessionId = sessionId;

            ConstructControlPacket(SNISMUXFlags.SMUX_SYN);
            if (!isAsync)
            {
                Write(_header.AsSpan());
            }
            else
            {
                await WriteAsync(_header.AsMemory(), ct).ConfigureAwait(false);
            }
        }

        private void ConstructControlPacket(SNISMUXFlags flags)
        {
            ushort sessionId = 0;
            _header[0] = 83;
            _header[1] = (byte)flags;
            _header[2] = (byte)(sessionId & 0xff); // BitConverter.GetBytes(_currentHeader.sessionId).CopyTo(headerBytes, 2);
            _header[3] = (byte)((sessionId >> 8) & 0xff);
            uint length = 0;
            _header[4] = (byte)(length & 0xff); // BitConverter.GetBytes(_currentHeader.length).CopyTo(headerBytes, 4);
            _header[5] = (byte)((length >> 8) & 0xff);
            _header[6] = (byte)((length >> 16) & 0xff);
            _header[7] = (byte)((length >> 24) & 0xff);

            uint sequenceNumber = 0;
            _header[8] = (byte)(sequenceNumber & 0xff); // BitConverter.GetBytes(_currentHeader.sequenceNumber).CopyTo(headerBytes, 8);
            _header[9] = (byte)((sequenceNumber >> 8) & 0xff);
            _header[10] = (byte)((sequenceNumber >> 16) & 0xff);
            _header[11] = (byte)((sequenceNumber >> 24) & 0xff);

            uint highwater= 4;
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
            _underLyingStream.Write(buffer, offset, count);
        }

        public override ValueTask WriteAsync(
            ReadOnlyMemory<byte> buffer,
            CancellationToken cancellationToken = default)
        {
            return _underLyingStream.WriteAsync(buffer, cancellationToken);
        }
    }
}
