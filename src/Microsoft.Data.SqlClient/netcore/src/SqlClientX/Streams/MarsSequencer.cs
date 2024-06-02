using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Data.SqlClient.SqlClientX.Streams
{
    internal class MarsSequencer
    {
        private int _sessionId;
        private int _sequenceNumber;

        public MarsSequencer()
        {
            _sequenceNumber = 0;
            _sessionId = 0;
        }

        public int NextSequenceNumber => Interlocked.Increment(ref this._sequenceNumber);

        public ushort NextSessionId
        {
            get
            {
                ushort sessionId = (ushort)_sessionId;
                Interlocked.Increment(ref _sessionId);
                return sessionId;
            }
        }
    }
}
