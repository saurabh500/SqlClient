// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Data.SqlClient;

internal enum Tokens : byte
{
    SQLERROR = TdsEnums.SQLERROR,
    SQLINFO = TdsEnums.SQLINFO,
    SQLENVCHANGE = TdsEnums.SQLENVCHANGE,
    SQLLOGINACK = TdsEnums.SQLLOGINACK,
    SQLSSPI = TdsEnums.SQLSSPI,
    SQLDONE = TdsEnums.SQLDONE,
    SQLDONEPROC = TdsEnums.SQLDONEPROC,
    SQLDONEINPROC = TdsEnums.SQLDONEINPROC,
    SQLFEATUREEXTACK = TdsEnums.SQLFEATUREEXTACK,
    SQLFEDAUTHINFO = TdsEnums.SQLFEDAUTHINFO,
    SQLSESSIONSTATE = TdsEnums.SQLSESSIONSTATE,
    SQLCOLMETADATA = TdsEnums.SQLCOLMETADATA,
    SQLROW = TdsEnums.SQLROW,
    SQLNBCROW = TdsEnums.SQLNBCROW,
    SQLRETURNSTATUS = TdsEnums.SQLRETURNSTATUS,
    SQLRETURNVALUE = TdsEnums.SQLRETURNVALUE,
    SQLTABNAME = TdsEnums.SQLTABNAME,
}
