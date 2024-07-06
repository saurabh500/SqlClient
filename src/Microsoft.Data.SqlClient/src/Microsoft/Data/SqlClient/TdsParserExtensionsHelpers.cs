using Microsoft.Data.SqlClient;

// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

internal static class TdsParserExtensionsHelpers
{

    internal static void WriteSqlVariantHeader(int length, byte tdstype, byte propbytes, TdsParserStateObject stateObj)
    {
        TdsParserExtensions.WriteInt(length, stateObj);
        stateObj.WriteByte(tdstype);
        stateObj.WriteByte(propbytes);
    }
}