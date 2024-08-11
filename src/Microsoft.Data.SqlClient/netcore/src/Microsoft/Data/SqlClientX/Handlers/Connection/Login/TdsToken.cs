// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Data.SqlClient;

internal enum TdsToken : byte
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

internal enum TdsFeature : byte
{
    FEATUREEXT_TERMINATOR = 0xFF,
    FEATUREEXT_SRECOVERY = 0x01,
    FEATUREEXT_FEDAUTH = 0x02,
    // 0x03 is for x_eFeatureExtensionId_Rcs
    FEATUREEXT_TCE = 0x04,
    FEATUREEXT_GLOBALTRANSACTIONS = 0x05,
    // 0x06 is for x_eFeatureExtensionId_LoginToken
    // 0x07 is for x_eFeatureExtensionId_ClientSideTelemetry
    FEATUREEXT_AZURESQLSUPPORT = 0x08,
    FEATUREEXT_DATACLASSIFICATION = 0x09,
    FEATUREEXT_UTF8SUPPORT = 0x0A,
    FEATUREEXT_SQLDNSCACHING = 0x0B,
}
