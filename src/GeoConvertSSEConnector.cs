using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Grpc.Core.Utils;
using NLog;
using Qlik.Sse;
using GeoUK;
using GeoUK.Projections;
using GeoUK.Coordinates;
using GeoUK.Ellipsoids;

namespace GeoConvertSSE
{
    /// <summary>
    /// The BasicExampleConnector inherits the generated class Qlik.Sse.Connector.ConnectorBase
    /// </summary>
    class BasicExampleConnector : Connector.ConnectorBase
    {
        private static readonly Logger Logger = LogManager.GetCurrentClassLogger();

        private enum FunctionConstant
        {
            Easting2Latitude,
            Northing2Longitude,
            Latitude2Easting,
            Longitude2Northing,
            BNG2WGS84,
            WGS842BNG
        };

        private static readonly Capabilities ConnectorCapabilities = new Capabilities
        {
            PluginIdentifier = "GeoConvertSSE",
            PluginVersion = "1.0.0",
            AllowScript = false,
            Functions =
            {
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.Easting2Latitude,
                    FunctionType = FunctionType.Scalar,
                    Name = "Easting2Latitude",
                    Params = {new Parameter {Name = "Easting", DataType = DataType.Numeric}, new Parameter {Name = "Northing", DataType = DataType.Numeric} },
                    ReturnType = DataType.Numeric
                },
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.Northing2Longitude,
                    FunctionType = FunctionType.Scalar,
                    Name = "Northing2Longitude",
                    Params = {new Parameter {Name = "Easting", DataType = DataType.Numeric}, new Parameter {Name = "Northing", DataType = DataType.Numeric} },
                    ReturnType = DataType.Numeric
                },
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.BNG2WGS84,
                    FunctionType = FunctionType.Tensor,
                    Name = "BNG2WGS84",
                    Params = { new Parameter { Name = "ID", DataType = DataType.Numeric }, new Parameter {Name = "Easting", DataType = DataType.Numeric}, new Parameter {Name = "Northing", DataType = DataType.Numeric} },
                    ReturnType = DataType.Numeric
                },
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.Latitude2Easting,
                    FunctionType = FunctionType.Scalar,
                    Name = "Latitude2Easting",
                    Params = {new Parameter {Name = "Latitude", DataType = DataType.Numeric}, new Parameter {Name = "Longitude", DataType = DataType.Numeric} },
                    ReturnType = DataType.Numeric
                },
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.Longitude2Northing,
                    FunctionType = FunctionType.Scalar,
                    Name = "Longitude2Northing",
                    Params = {new Parameter {Name = "Latitude", DataType = DataType.Numeric}, new Parameter {Name = "Longitude", DataType = DataType.Numeric} },
                    ReturnType = DataType.Numeric
                },
                new FunctionDefinition {
                    FunctionId = (int)FunctionConstant.WGS842BNG,
                    FunctionType = FunctionType.Tensor,
                    Name = "WGS842BNG",
                    Params = { new Parameter { Name = "ID", DataType = DataType.Numeric }, new Parameter {Name = "Latitude", DataType = DataType.Numeric}, new Parameter {Name = "Longitude", DataType = DataType.Numeric} }, 
                    ReturnType = DataType.Numeric
                },
            }
        };

        public override Task<Capabilities> GetCapabilities(Empty request, ServerCallContext context)
        {
            if (Logger.IsTraceEnabled)
            {
                Logger.Trace("-- GetCapabilities --");

                TraceServerCallContext(context);
            }
            else
            {
                Logger.Debug("GetCapabilites called");
            }

            return Task.FromResult(ConnectorCapabilities);
        }

        private LatitudeLongitude OS2WGS84 (double easting, double northing)
        {
            var cartesian = GeoUK.Convert.ToCartesian(new Airy1830(),
                                    new BritishNationalGrid(),
                                    new EastingNorthing(
                                        easting, northing));

            var wgsCartesian = Transform.Osgb36ToEtrs89(cartesian);
            return GeoUK.Convert.ToLatitudeLongitude(new Wgs84(),
                            wgsCartesian);
        }

        private EastingNorthing WGS842OS(double latitude, double longitude)
        {
            var latLong = new LatitudeLongitude(latitude, longitude);

            var cartesian = GeoUK.Convert.ToCartesian(new Wgs84(), latLong);
            var bngCartesian = Transform.Etrs89ToOsgb36(cartesian);
            return GeoUK.Convert.ToEastingNorthing(new Airy1830(), new BritishNationalGrid(), bngCartesian);
        }

        public override async Task ExecuteFunction(IAsyncStreamReader<BundledRows> requestStream, IServerStreamWriter<BundledRows> responseStream, ServerCallContext context)
        {
            if (Logger.IsTraceEnabled)
            {
                Logger.Trace("-- ExecuteFunction --");

                TraceServerCallContext(context);
            }
            else
            {
                Logger.Debug("ExecuteFunction called");
            }

            var functionRequestHeaderStream = context.RequestHeaders.SingleOrDefault(header => header.Key == "qlik-functionrequestheader-bin");

            if (functionRequestHeaderStream == null)
            {
                throw new Exception("ExecuteFunction called without Function Request Header in Request Headers.");
            }

            var functionRequestHeader = new FunctionRequestHeader();
            functionRequestHeader.MergeFrom(new CodedInputStream(functionRequestHeaderStream.ValueBytes));

            Logger.Trace($"FunctionRequestHeader.FunctionId String : {(FunctionConstant)functionRequestHeader.FunctionId}");

            

            switch (functionRequestHeader.FunctionId)
            {
                case (int)FunctionConstant.Easting2Latitude:
                    {
                        while (await requestStream.MoveNext())
                        {
                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();

                                var wgsLatLong = OS2WGS84(row.Duals[0].NumData, row.Duals[1].NumData);

                                resultRow.Duals.Add(new Dual { NumData = wgsLatLong.Latitude });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }

                        break;
                    }
                case (int)FunctionConstant.Northing2Longitude:
                    {
                        while (await requestStream.MoveNext())
                        {
                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();
                                var wgsLatLong = OS2WGS84(row.Duals[0].NumData, row.Duals[1].NumData);

                                resultRow.Duals.Add(new Dual { NumData = wgsLatLong.Longitude });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }

                        break;
                    }
                case (int)FunctionConstant.BNG2WGS84:
                    {
                        while (await requestStream.MoveNext())
                        {

                            TableDescription tableDesc = new TableDescription();
                            tableDesc.NumberOfRows = requestStream.Current.Rows.Count;
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Dual, Name = "ID" });
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Numeric, Name = "Latitude" });
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Numeric, Name = "Longitude" });

                            var tableMetadata = new Metadata
                            {
                                { new Metadata.Entry("qlik-tabledescription-bin", MessageExtensions.ToByteArray(tableDesc)) }
                            };

                            await context.WriteResponseHeadersAsync(tableMetadata);

                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();
                                var wgsLatLong = OS2WGS84(row.Duals[1].NumData, row.Duals[2].NumData);

                                resultRow.Duals.Add(row.Duals[0]);
                                resultRow.Duals.Add(new Dual { NumData = wgsLatLong.Latitude });
                                resultRow.Duals.Add(new Dual { NumData = wgsLatLong.Longitude });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }
                        break;
                    }

                case (int)FunctionConstant.Latitude2Easting:
                    {
                        while (await requestStream.MoveNext())
                        {
                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();

                                var geo = WGS842OS(row.Duals[0].NumData, row.Duals[1].NumData);

                                resultRow.Duals.Add(new Dual { NumData =geo.Easting });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }

                        break;
                    }
                case (int)FunctionConstant.Longitude2Northing:
                    {
                        while (await requestStream.MoveNext())
                        {
                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();
                                var geo = WGS842OS(row.Duals[0].NumData, row.Duals[1].NumData);

                                resultRow.Duals.Add(new Dual { NumData = geo.Northing });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }

                        break;
                    }
                case (int)FunctionConstant.WGS842BNG:
                    {
                        Logger.Trace("WGS842BNG");
                        Logger.Trace($"WGS842BNG {requestStream}");
                        while (await requestStream.MoveNext())
                        {
                            Logger.Trace($"WGS842BNG {requestStream.Current.Rows.Count}");
                            TableDescription tableDesc = new TableDescription();
                            tableDesc.NumberOfRows = requestStream.Current.Rows.Count;
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Dual, Name = "ID" });
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Numeric, Name = "Easting" });
                            tableDesc.Fields.Add(new FieldDescription { DataType = DataType.Numeric, Name = "Northing" });

                            var tableMetadata = new Metadata
                            {
                                { new Metadata.Entry("qlik-tabledescription-bin", MessageExtensions.ToByteArray(tableDesc)) }
                            };

                            await context.WriteResponseHeadersAsync(tableMetadata);

                            var resultBundle = new BundledRows();

                            foreach (var row in requestStream.Current.Rows)
                            {
                                var resultRow = new Row();
                                Logger.Trace($"Row: {row.Duals[1].NumData} {row.Duals[2].NumData}");
                                var geo = WGS842OS(row.Duals[1].NumData, row.Duals[2].NumData);

                                resultRow.Duals.Add(row.Duals[0]);
                                resultRow.Duals.Add(new Dual { NumData = geo.Easting });
                                resultRow.Duals.Add(new Dual { NumData = geo.Northing });
                                resultBundle.Rows.Add(resultRow);
                            }
                            await responseStream.WriteAsync(resultBundle);
                        }
                        Logger.Trace("WGS842BNG END");
                        break;
                    }

                default:
                    break;
            }

            Logger.Trace("-- (ExecuteFunction) --");
        }

        private static long _callCounter = 0;

        private static void TraceServerCallContext(ServerCallContext context)
        {
            var authContext = context.AuthContext;

            Logger.Trace($"ServerCallContext.Method : {context.Method}");
            Logger.Trace($"ServerCallContext.Host : {context.Host}");
            Logger.Trace($"ServerCallContext.Peer : {context.Peer}");
            foreach (var contextRequestHeader in context.RequestHeaders)
            {
                Logger.Trace(
                    $"{contextRequestHeader.Key} : {(contextRequestHeader.IsBinary ? "<binary>" : contextRequestHeader.Value)}");

                if (contextRequestHeader.Key == "qlik-functionrequestheader-bin")
                {
                    var functionRequestHeader = new FunctionRequestHeader();
                    functionRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"FunctionRequestHeader.FunctionId : {functionRequestHeader.FunctionId}");
                    Logger.Trace($"FunctionRequestHeader.Version : {functionRequestHeader.Version}");
                }
                else if (contextRequestHeader.Key == "qlik-commonrequestheader-bin")
                {
                    var commonRequestHeader = new CommonRequestHeader();
                    commonRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"CommonRequestHeader.AppId : {commonRequestHeader.AppId}");
                    Logger.Trace($"CommonRequestHeader.Cardinality : {commonRequestHeader.Cardinality}");
                    Logger.Trace($"CommonRequestHeader.UserId : {commonRequestHeader.UserId}");
                }
                else if (contextRequestHeader.Key == "qlik-scriptrequestheader-bin")
                {
                    var scriptRequestHeader = new ScriptRequestHeader();
                    scriptRequestHeader.MergeFrom(new CodedInputStream(contextRequestHeader.ValueBytes));

                    Logger.Trace($"ScriptRequestHeader.FunctionType : {scriptRequestHeader.FunctionType}");
                    Logger.Trace($"ScriptRequestHeader.ReturnType : {scriptRequestHeader.ReturnType}");

                    int paramIdx = 0;

                    foreach (var parameter in scriptRequestHeader.Params)
                    {
                        Logger.Trace($"ScriptRequestHeader.Params[{paramIdx}].Name : {parameter.Name}");
                        Logger.Trace($"ScriptRequestHeader.Params[{paramIdx}].DataType : {parameter.DataType}");
                        ++paramIdx;
                    }
                    Logger.Trace($"CommonRequestHeader.Script : {scriptRequestHeader.Script}");
                }
            }

            Logger.Trace($"ServerCallContext.AuthContext.IsPeerAuthenticated : {authContext.IsPeerAuthenticated}");
            Logger.Trace(
                $"ServerCallContext.AuthContext.PeerIdentityPropertyName : {authContext.PeerIdentityPropertyName}");
            foreach (var authContextProperty in authContext.Properties)
            {
                var loggedValue = authContextProperty.Value;
                var firstLineLength = loggedValue.IndexOf('\n');

                if (firstLineLength > 0)
                {
                    loggedValue = loggedValue.Substring(0, firstLineLength) + "<truncated at linefeed>";
                }

                Logger.Trace($"{authContextProperty.Name} : {loggedValue}");
            }
        }
    }
}