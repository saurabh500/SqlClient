// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

namespace Microsoft.Data.SqlClientX.Handlers.Connection.Login
{
    internal class TdsError
    {
        private int _errorNumber;
        private byte _errorState;
        private byte _errorClass;
        private string _message;
        private string _server;
        private string _proc;
        private int _lineNumber;

        public TdsError(int errorNumber, byte errorState, byte errorClass, string message, string server, string proc, int lineNumber)
        {
            _errorNumber = errorNumber;
            _errorState = errorState;
            _errorClass = errorClass;
            _message = message;
            _server = server;
            _proc = proc;
            _lineNumber = lineNumber;
        }
    }
}
