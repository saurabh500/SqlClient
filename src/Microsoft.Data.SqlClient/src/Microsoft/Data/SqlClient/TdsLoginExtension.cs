// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.
// See the LICENSE file in the project root for more information.

using Microsoft.Data.Common;
using System;

namespace Microsoft.Data.SqlClient
{
    internal static class TdsLoginExtension
    {

        internal static void WriteLoginData(SqlLogin rec,
                                    TdsEnums.FeatureExtension requestedFeatures,
                                    SessionData recoverySessionData,
                                    FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData,
                                    SqlConnectionEncryptOption encrypt,
                                    byte[] encryptedPassword,
                                    byte[] encryptedChangePassword,
                                    int encryptedPasswordLengthInBytes,
                                    int encryptedChangePasswordLengthInBytes,
                                    bool useFeatureExt,
                                    string userName,
                                    int length,
                                    int featureExOffset,
                                    string clientInterfaceName,
                                    byte[] outSSPIBuff,
                                    uint outSSPILength,
                                     TdsParserStateObject _physicalStateObj,
                                     SqlInternalConnectionTds _connHandler,
                                     int ObjectID,
                                     byte[] s_nicAddress)
        {
#if NETFRAMEWORK
            WriteLoginDataNetFx(rec,
                           requestedFeatures,
                           recoverySessionData,
                           fedAuthFeatureExtensionData,
                           encrypt,
                           encryptedPassword,
                           encryptedChangePassword,
                           encryptedPasswordLengthInBytes,
                           encryptedChangePasswordLengthInBytes,
                           useFeatureExt,
                           userName,
                           length,
                           featureExOffset,
                           clientInterfaceName,
                           outSSPIBuff,
                           outSSPILength,
                           _physicalStateObj,
                           _connHandler,
                           ObjectID,
                           s_nicAddress);
#else
            WriteLoginDataNetCore(rec,
                           requestedFeatures,
                           recoverySessionData,
                           fedAuthFeatureExtensionData,
                           encrypt,
                           encryptedPassword,
                           encryptedChangePassword,
                           encryptedPasswordLengthInBytes,
                           encryptedChangePasswordLengthInBytes,
                           useFeatureExt,
                           userName,
                           length,
                           featureExOffset,
                           clientInterfaceName,
                           outSSPIBuff,
                           outSSPILength,
                           _physicalStateObj,
                           _connHandler,
                           ObjectID,
                           s_nicAddress);
#endif
        }

#if NET6_0_OR_GREATER
        private static void WriteLoginDataNetCore(SqlLogin rec,
                                     TdsEnums.FeatureExtension requestedFeatures,
                                     SessionData recoverySessionData,
                                     FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData,
                                     SqlConnectionEncryptOption encrypt,
                                     byte[] encryptedPassword,
                                     byte[] encryptedChangePassword,
                                     int encryptedPasswordLengthInBytes,
                                     int encryptedChangePasswordLengthInBytes,
                                     bool useFeatureExt,
                                     string userName,
                                     int length,
                                     int featureExOffset,
                                     string clientInterfaceName,
                                     byte[] outSSPIBuff,
                                     uint outSSPILength,
                                     TdsParserStateObject _physicalStateObj,
                                     SqlInternalConnectionTds _connHandler,
                                     int ObjectID,
                                     byte[] s_nicAddress)
        {
            try
            {
                TdsParserExtensions.WriteInt(length, _physicalStateObj);
                if (recoverySessionData == null)
                {
                    if (encrypt == SqlConnectionEncryptOption.Strict)
                    {
                        TdsParserExtensions.WriteInt((TdsEnums.TDS8_MAJOR << 24) | (TdsEnums.TDS8_INCREMENT << 16) | TdsEnums.TDS8_MINOR, _physicalStateObj);
                    }
                    else
                    {
                        TdsParserExtensions.WriteInt((TdsEnums.SQL2012_MAJOR << 24) | (TdsEnums.SQL2012_INCREMENT << 16) | TdsEnums.SQL2012_MINOR, _physicalStateObj);
                    }
                }
                else
                {
                    TdsParserExtensions.WriteUnsignedInt(recoverySessionData._tdsVersion, _physicalStateObj);
                }
                TdsParserExtensions.WriteInt(rec.packetSize, _physicalStateObj);
                TdsParserExtensions.WriteInt(TdsEnums.CLIENT_PROG_VER, _physicalStateObj);
                TdsParserExtensions.WriteInt(TdsParserStaticMethods.GetCurrentProcessIdForTdsLoginOnly(), _physicalStateObj);
                TdsParserExtensions.WriteInt(0, _physicalStateObj); // connectionID is unused

                // Log7Flags (DWORD)
                int log7Flags = 0;

                /*
                 Current snapshot from TDS spec with the offsets added:
                    0) fByteOrder:1,                // byte order of numeric data types on client
                    1) fCharSet:1,                  // character set on client
                    2) fFloat:2,                    // Type of floating point on client
                    4) fDumpLoad:1,                 // Dump/Load and BCP enable
                    5) fUseDb:1,                    // USE notification
                    6) fDatabase:1,                 // Initial database fatal flag
                    7) fSetLang:1,                  // SET LANGUAGE notification
                    8) fLanguage:1,                 // Initial language fatal flag
                    9) fODBC:1,                     // Set if client is ODBC driver
                   10) fTranBoundary:1,             // Transaction boundary notification
                   11) fDelegatedSec:1,             // Security with delegation is available
                   12) fUserType:3,                 // Type of user
                   15) fIntegratedSecurity:1,       // Set if client is using integrated security
                   16) fSQLType:4,                  // Type of SQL sent from client
                   20) fOLEDB:1,                    // Set if client is OLEDB driver
                   21) fSpare1:3,                   // first bit used for read-only intent, rest unused
                   24) fResetPassword:1,            // set if client wants to reset password
                   25) fNoNBCAndSparse:1,           // set if client does not support NBC and Sparse column
                   26) fUserInstance:1,             // This connection wants to connect to a SQL "user instance"
                   27) fUnknownCollationHandling:1, // This connection can handle unknown collation correctly.
                   28) fExtension:1                 // Extensions are used
                   32 - total
                */

                // first byte
                log7Flags |= TdsEnums.USE_DB_ON << 5;
                log7Flags |= TdsEnums.INIT_DB_FATAL << 6;
                log7Flags |= TdsEnums.SET_LANG_ON << 7;

                // second byte
                log7Flags |= TdsEnums.INIT_LANG_FATAL << 8;
                log7Flags |= TdsEnums.ODBC_ON << 9;
                if (rec.useReplication)
                {
                    log7Flags |= TdsEnums.REPL_ON << 12;
                }
                if (rec.useSSPI)
                {
                    log7Flags |= TdsEnums.SSPI_ON << 15;
                }

                // third byte
                if (rec.readOnlyIntent)
                {
                    log7Flags |= TdsEnums.READONLY_INTENT_ON << 21; // read-only intent flag is a first bit of fSpare1
                }

                // 4th one
                if (!string.IsNullOrEmpty(rec.newPassword) || (rec.newSecurePassword != null && rec.newSecurePassword.Length != 0))
                {
                    log7Flags |= 1 << 24;
                }
                if (rec.userInstance)
                {
                    log7Flags |= 1 << 26;
                }
                if (useFeatureExt)
                {
                    log7Flags |= 1 << 28;
                }

                TdsParserExtensions.WriteInt(log7Flags, _physicalStateObj);
                SqlClientEventSource.Log.TryAdvancedTraceEvent("<sc.TdsParser.TdsLogin|ADV> {0}, TDS Login7 flags = {1}:", ObjectID, log7Flags);

                TdsParserExtensions.WriteInt(0, _physicalStateObj);  // ClientTimeZone is not used
                TdsParserExtensions.WriteInt(0, _physicalStateObj);  // LCID is unused by server

                // Start writing offset and length of variable length portions
                int offset = TdsEnums.SQL2005_LOG_REC_FIXED_LEN;

                // write offset/length pairs

                // note that you must always set ibHostName since it indicates the beginning of the variable length section of the login record
                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // host name offset
                TdsParserExtensions.WriteShort(rec.hostName.Length, _physicalStateObj);
                offset += rec.hostName.Length * 2;

                // Only send user/password over if not fSSPI...  If both user/password and SSPI are in login
                // rec, only SSPI is used.  Confirmed same behavior as in luxor.
                if (!rec.useSSPI && !(_connHandler.Features.FedAuth.IsInfoRequested || _connHandler.Features.FedAuth.IsRequested))
                {
                    TdsParserExtensions.WriteShort(offset, _physicalStateObj);  // userName offset
                    TdsParserExtensions.WriteShort(userName.Length, _physicalStateObj);
                    offset += userName.Length * 2;

                    // the encrypted password is a byte array - so length computations different than strings
                    TdsParserExtensions.WriteShort(offset, _physicalStateObj); // password offset
                    TdsParserExtensions.WriteShort(encryptedPasswordLengthInBytes / 2, _physicalStateObj);
                    offset += encryptedPasswordLengthInBytes;
                }
                else
                {
                    // case where user/password data is not used, send over zeros
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);  // userName offset
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);  // password offset
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // app name offset
                TdsParserExtensions.WriteShort(rec.applicationName.Length, _physicalStateObj);
                offset += rec.applicationName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // server name offset
                TdsParserExtensions.WriteShort(rec.serverName.Length, _physicalStateObj);
                offset += rec.serverName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj);
                if (useFeatureExt)
                {
                    TdsParserExtensions.WriteShort(4, _physicalStateObj); // length of ibFeatgureExtLong (which is a DWORD)
                    offset += 4;
                }
                else
                {
                    TdsParserExtensions.WriteShort(0, _physicalStateObj); // unused (was remote password ?)
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // client interface name offset
                TdsParserExtensions.WriteShort(clientInterfaceName.Length, _physicalStateObj);
                offset += clientInterfaceName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // language name offset
                TdsParserExtensions.WriteShort(rec.language.Length, _physicalStateObj);
                offset += rec.language.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // database name offset
                TdsParserExtensions.WriteShort(rec.database.Length, _physicalStateObj);
                offset += rec.database.Length * 2;

                _physicalStateObj.WriteByteArray(s_nicAddress, s_nicAddress.Length, 0);

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // ibSSPI offset
                if (rec.useSSPI)
                {
                    TdsParserExtensions.WriteShort((int)outSSPILength, _physicalStateObj);
                    offset += (int)outSSPILength;
                }
                else
                {
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // DB filename offset
                TdsParserExtensions.WriteShort(rec.attachDBFilename.Length, _physicalStateObj);
                offset += rec.attachDBFilename.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // reset password offset
                TdsParserExtensions.WriteShort(encryptedChangePasswordLengthInBytes / 2, _physicalStateObj);

                TdsParserExtensions.WriteInt(0, _physicalStateObj);        // reserved for chSSPI

                // write variable length portion
                TdsParserExtensions.WriteString(rec.hostName, _physicalStateObj);

                // if we are using SSPI, do not send over username/password, since we will use SSPI instead
                // same behavior as Luxor
                if (!rec.useSSPI && !(_connHandler.Features.FedAuth.IsInfoRequested || _connHandler.Features.FedAuth.IsRequested))
                {
                    TdsParserExtensions.WriteString(userName, _physicalStateObj);

                    if (rec.credential != null)
                    {
                        _physicalStateObj.WriteSecureString(rec.credential.Password);
                    }
                    else
                    {
                        _physicalStateObj.WriteByteArray(encryptedPassword, encryptedPasswordLengthInBytes, 0);
                    }
                }

                TdsParserExtensions.WriteString(rec.applicationName, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.serverName, _physicalStateObj);

                // write ibFeatureExtLong
                if (useFeatureExt)
                {
                    if ((requestedFeatures & TdsEnums.FeatureExtension.FedAuth) != 0)
                    {
                        SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TdsLogin|SEC> Sending federated authentication feature request");
                    }

                    TdsParserExtensions.WriteInt(featureExOffset, _physicalStateObj);
                }

                TdsParserExtensions.WriteString(clientInterfaceName, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.language, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.database, _physicalStateObj);

                // send over SSPI data if we are using SSPI
                if (rec.useSSPI)
                    _physicalStateObj.WriteByteArray(outSSPIBuff, (int)outSSPILength, 0);

                TdsParserExtensions.WriteString(rec.attachDBFilename, _physicalStateObj);
                if (!rec.useSSPI && !(_connHandler.Features.FedAuth.IsInfoRequested || 
                    _connHandler.Features.FedAuth.IsRequested))
                {
                    if (rec.newSecurePassword != null)
                    {
                        _physicalStateObj.WriteSecureString(rec.newSecurePassword);
                    }
                    else
                    {
                        _physicalStateObj.WriteByteArray(encryptedChangePassword, encryptedChangePasswordLengthInBytes, 0);
                    }
                }

                TdsFeaturesHandler.ApplyFeatureExData(requestedFeatures, recoverySessionData, fedAuthFeatureExtensionData, useFeatureExt, length, _physicalStateObj, _connHandler, true);
            }
            catch (Exception e)
            {
                if (ADP.IsCatchableExceptionType(e))
                {
                    // be sure to wipe out our buffer if we started sending stuff
                    _physicalStateObj.ResetPacketCounters();
                    _physicalStateObj.ResetBuffer();
                }

                throw;
            }
        }
#endif

#if NETFRAMEWORK
        internal static void WriteLoginDataNetFx(SqlLogin rec,
                                    TdsEnums.FeatureExtension requestedFeatures,
                                    SessionData recoverySessionData,
                                    FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData,
                                    SqlConnectionEncryptOption encrypt,
                                    byte[] encryptedPassword,
                                    byte[] encryptedChangePassword,
                                    int encryptedPasswordLengthInBytes,
                                    int encryptedChangePasswordLengthInBytes,
                                    bool useFeatureExt,
                                    string userName,
                                    int length,
                                    int featureExOffset,
                                    string clientInterfaceName,
                                    byte[] outSSPIBuff,
                                    uint outSSPILength,
                                     TdsParserStateObject _physicalStateObj,
                                     SqlInternalConnectionTds _connHandler,
                                     int ObjectID,
                                     byte[] s_nicAddress)
        {
            try
            {
                TdsParserExtensions.WriteInt(length, _physicalStateObj);
                if (recoverySessionData == null)
                {
                    if (encrypt == SqlConnectionEncryptOption.Strict)
                    {
                        TdsParserExtensions.WriteInt((TdsEnums.TDS8_MAJOR << 24) | (TdsEnums.TDS8_INCREMENT << 16) | TdsEnums.TDS8_MINOR, _physicalStateObj);
                    }
                    else
                    {
                        TdsParserExtensions.WriteInt((TdsEnums.SQL2012_MAJOR << 24) | (TdsEnums.SQL2012_INCREMENT << 16) | TdsEnums.SQL2012_MINOR, _physicalStateObj);
                    }
                }
                else
                {
                    TdsParserExtensions.WriteUnsignedInt(recoverySessionData._tdsVersion, _physicalStateObj);
                }
                TdsParserExtensions.WriteInt(rec.packetSize, _physicalStateObj);
                TdsParserExtensions.WriteInt(TdsEnums.CLIENT_PROG_VER, _physicalStateObj);
                TdsParserExtensions.WriteInt(TdsParserStaticMethods.GetCurrentProcessIdForTdsLoginOnly(), _physicalStateObj); //MDAC 84718
                TdsParserExtensions.WriteInt(0, _physicalStateObj); // connectionID is unused

                // Log7Flags (DWORD)
                int log7Flags = 0;

                /*
                 Current snapshot from TDS spec with the offsets added:
                    0) fByteOrder:1,                // byte order of numeric data types on client
                    1) fCharSet:1,                  // character set on client
                    2) fFloat:2,                    // Type of floating point on client
                    4) fDumpLoad:1,                 // Dump/Load and BCP enable
                    5) fUseDb:1,                    // USE notification
                    6) fDatabase:1,                 // Initial database fatal flag
                    7) fSetLang:1,                  // SET LANGUAGE notification
                    8) fLanguage:1,                 // Initial language fatal flag
                    9) fODBC:1,                     // Set if client is ODBC driver
                   10) fTranBoundary:1,             // Transaction boundary notification
                   11) fDelegatedSec:1,             // Security with delegation is available
                   12) fUserType:3,                 // Type of user
                   15) fIntegratedSecurity:1,       // Set if client is using integrated security
                   16) fSQLType:4,                  // Type of SQL sent from client
                   20) fOLEDB:1,                    // Set if client is OLEDB driver
                   21) fSpare1:3,                   // first bit used for read-only intent, rest unused
                   24) fResetPassword:1,            // set if client wants to reset password
                   25) fNoNBCAndSparse:1,           // set if client does not support NBC and Sparse column
                   26) fUserInstance:1,             // This connection wants to connect to a SQL "user instance"
                   27) fUnknownCollationHandling:1, // This connection can handle unknown collation correctly.
                   28) fExtension:1                 // Extensions are used
                   32 - total
                */

                // first byte
                log7Flags |= TdsEnums.USE_DB_ON << 5;
                log7Flags |= TdsEnums.INIT_DB_FATAL << 6;
                log7Flags |= TdsEnums.SET_LANG_ON << 7;

                // second byte
                log7Flags |= TdsEnums.INIT_LANG_FATAL << 8;
                log7Flags |= TdsEnums.ODBC_ON << 9;
                if (rec.useReplication)
                {
                    log7Flags |= TdsEnums.REPL_ON << 12;
                }
                if (rec.useSSPI)
                {
                    log7Flags |= TdsEnums.SSPI_ON << 15;
                }

                // third byte
                if (rec.readOnlyIntent)
                {
                    log7Flags |= TdsEnums.READONLY_INTENT_ON << 21; // read-only intent flag is a first bit of fSpare1
                }

                // 4th one
                if (!ADP.IsEmpty(rec.newPassword) || (rec.newSecurePassword != null && rec.newSecurePassword.Length != 0))
                {
                    log7Flags |= 1 << 24;
                }
                if (rec.userInstance)
                {
                    log7Flags |= 1 << 26;
                }

                if (useFeatureExt)
                {
                    log7Flags |= 1 << 28;
                }

                TdsParserExtensions.WriteInt(log7Flags, _physicalStateObj);
                SqlClientEventSource.Log.TryAdvancedTraceEvent("<sc.TdsParser.TdsLogin|ADV> {0}, TDS Login7 flags = {1}:", ObjectID, log7Flags);

                TdsParserExtensions.WriteInt(0, _physicalStateObj);  // ClientTimeZone is not used
                TdsParserExtensions.WriteInt(0, _physicalStateObj);  // LCID is unused by server

                // Start writing offset and length of variable length portions
                int offset = TdsEnums.SQL2005_LOG_REC_FIXED_LEN;

                // write offset/length pairs

                // note that you must always set ibHostName since it indicaters the beginning of the variable length section of the login record
                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // host name offset
                TdsParserExtensions.WriteShort(rec.hostName.Length, _physicalStateObj);
                offset += rec.hostName.Length * 2;

                // Only send user/password over if not fSSPI or fed auth MSAL...  If both user/password and SSPI are in login
                // rec, only SSPI is used.  Confirmed same bahavior as in luxor.
                if (!rec.useSSPI && !(_connHandler._federatedAuthenticationInfoRequested || _connHandler._federatedAuthenticationRequested))
                {
                    TdsParserExtensions.WriteShort(offset, _physicalStateObj);  // userName offset
                    TdsParserExtensions.WriteShort(userName.Length, _physicalStateObj);
                    offset += userName.Length * 2;

                    // the encrypted password is a byte array - so length computations different than strings
                    TdsParserExtensions.WriteShort(offset, _physicalStateObj); // password offset
                    TdsParserExtensions.WriteShort(encryptedPasswordLengthInBytes / 2, _physicalStateObj);
                    offset += encryptedPasswordLengthInBytes;
                }
                else
                {
                    // case where user/password data is not used, send over zeros
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);  // userName offset
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);  // password offset
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // app name offset
                TdsParserExtensions.WriteShort(rec.applicationName.Length, _physicalStateObj);
                offset += rec.applicationName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // server name offset
                TdsParserExtensions.WriteShort(rec.serverName.Length, _physicalStateObj);
                offset += rec.serverName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj);
                if (useFeatureExt)
                {
                    TdsParserExtensions.WriteShort(4, _physicalStateObj); // length of ibFeatgureExtLong (which is a DWORD)
                    offset += 4;
                }
                else
                {
                    TdsParserExtensions.WriteShort(0, _physicalStateObj); // unused (was remote password ?)
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // client interface name offset
                TdsParserExtensions.WriteShort(clientInterfaceName.Length, _physicalStateObj);
                offset += clientInterfaceName.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // language name offset
                TdsParserExtensions.WriteShort(rec.language.Length, _physicalStateObj);
                offset += rec.language.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // database name offset
                TdsParserExtensions.WriteShort(rec.database.Length, _physicalStateObj);
                offset += rec.database.Length * 2;

                // UNDONE: NIC address
                // previously we declared the array and simply sent it over - byte[] of 0's
                if (null == s_nicAddress)
                    s_nicAddress = TdsParserStaticMethods.GetNetworkPhysicalAddressForTdsLoginOnly();

                _physicalStateObj.WriteByteArray(s_nicAddress, s_nicAddress.Length, 0);

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // ibSSPI offset
                if (rec.useSSPI)
                {
                    TdsParserExtensions.WriteShort((int)outSSPILength, _physicalStateObj);
                    offset += (int)outSSPILength;
                }
                else
                {
                    TdsParserExtensions.WriteShort(0, _physicalStateObj);
                }

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // DB filename offset
                TdsParserExtensions.WriteShort(rec.attachDBFilename.Length, _physicalStateObj);
                offset += rec.attachDBFilename.Length * 2;

                TdsParserExtensions.WriteShort(offset, _physicalStateObj); // reset password offset
                TdsParserExtensions.WriteShort(encryptedChangePasswordLengthInBytes / 2, _physicalStateObj);

                TdsParserExtensions.WriteInt(0, _physicalStateObj);        // reserved for chSSPI

                // write variable length portion
                TdsParserExtensions.WriteString(rec.hostName, _physicalStateObj);

                // if we are using SSPI or fed auth MSAL, do not send over username/password, since we will use SSPI instead
                // same behavior as Luxor
                if (!rec.useSSPI && !(_connHandler._federatedAuthenticationInfoRequested || _connHandler._federatedAuthenticationRequested))
                {
                    TdsParserExtensions.WriteString(userName, _physicalStateObj);

                    // Cache offset in packet for tracing.
                    _physicalStateObj._tracePasswordOffset = _physicalStateObj._outBytesUsed;
                    _physicalStateObj._tracePasswordLength = encryptedPasswordLengthInBytes;

                    if (rec.credential != null)
                    {
                        _physicalStateObj.WriteSecureString(rec.credential.Password);
                    }
                    else
                    {
                        _physicalStateObj.WriteByteArray(encryptedPassword, encryptedPasswordLengthInBytes, 0);
                    }
                }

                TdsParserExtensions.WriteString(rec.applicationName, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.serverName, _physicalStateObj);

                // write ibFeatureExtLong
                if (useFeatureExt)
                {
                    if ((requestedFeatures & TdsEnums.FeatureExtension.FedAuth) != 0)
                    {
                        SqlClientEventSource.Log.TryTraceEvent("<sc.TdsParser.TdsLogin|SEC> Sending federated authentication feature request");
                    }

                    TdsParserExtensions.WriteInt(featureExOffset, _physicalStateObj);
                }

                TdsParserExtensions.WriteString(clientInterfaceName, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.language, _physicalStateObj);
                TdsParserExtensions.WriteString(rec.database, _physicalStateObj);

                // send over SSPI data if we are using SSPI
                if (rec.useSSPI)
                    _physicalStateObj.WriteByteArray(outSSPIBuff, (int)outSSPILength, 0);

                TdsParserExtensions.WriteString(rec.attachDBFilename, _physicalStateObj);
                if (!rec.useSSPI && !(_connHandler._federatedAuthenticationInfoRequested || _connHandler._federatedAuthenticationRequested))
                {
                    // Cache offset in packet for tracing.
                    _physicalStateObj._traceChangePasswordOffset = _physicalStateObj._outBytesUsed;
                    _physicalStateObj._traceChangePasswordLength = encryptedChangePasswordLengthInBytes;
                    if (rec.newSecurePassword != null)
                    {
                        _physicalStateObj.WriteSecureString(rec.newSecurePassword);
                    }
                    else
                    {
                        _physicalStateObj.WriteByteArray(encryptedChangePassword, encryptedChangePasswordLengthInBytes, 0);
                    }
                }

                TdsFeaturesHandler.ApplyFeatureExData(requestedFeatures,
                    recoverySessionData,
                    fedAuthFeatureExtensionData,
                    useFeatureExt,
                    length,
                    _physicalStateObj,
                    _connHandler,
                    true);
            } // try
            catch (Exception e)
            {
                // UNDONE - should not be catching all exceptions!!!
                if (ADP.IsCatchableExceptionType(e))
                {
                    // be sure to wipe out our buffer if we started sending stuff
                    _physicalStateObj.ResetPacketCounters();
                    _physicalStateObj.ResetBuffer();
                }

                throw;
            }
        }
#endif
    }
}
