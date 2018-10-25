namespace DevExpress.Xpo.DB {
    using System;
    using System.Data;
    using System.Text;
    using System.Collections;
    using System.Collections.Specialized;
    using System.Globalization;
    using DevExpress.Utils;
    using DevExpress.Data.Filtering;
    using DevExpress.Xpo.DB.Exceptions;
    using DevExpress.Xpo.DB.Helpers;
    using System.Collections.Generic;

    public class AseClientConnectionProvider : ConnectionProviderSql {
        public const string XpoProviderTypeString = "AseClient";
        const string MulticolumnIndexesAreNotSupported = "Multicolumn indexes are not supported.";
        ReflectConnectionHelper helper;

        ReflectConnectionHelper ConnectionHelper {
            get {
                if(helper == null)
                    helper = new ReflectConnectionHelper(Connection, AseExceptionName);
                return helper;
            }
        }
        public static bool? GlobalExecuteUpdateSchemaInTransaction;
        public bool? ExecuteUpdateSchemaInTransaction = true;
        protected override bool InternalExecuteUpdateSchemaInTransaction {
            get {
                if(ExecuteUpdateSchemaInTransaction.HasValue || GlobalExecuteUpdateSchemaInTransaction.HasValue) {
                    return ExecuteUpdateSchemaInTransaction.HasValue ? ExecuteUpdateSchemaInTransaction.Value : GlobalExecuteUpdateSchemaInTransaction.Value;
                } else {
                    return false;
                }
            }
        }
        public static string GetConnectionString(string server, string database, string userId, string password) {
            return GetConnectionString(server, 5000, database, userId, password);
        }
        public static string GetConnectionString(string server, int port, string database, string userId, string password) {
            return String.Format("{5}={6};Port={4};Data Source={0};User ID={1};Password={2};Initial Catalog={3};persist security info=true;Pooling=false",
               EscapeConnectionStringArgument(server), EscapeConnectionStringArgument(userId), EscapeConnectionStringArgument(password), EscapeConnectionStringArgument(database), port, DataStoreBase.XpoProviderTypeParameterName, XpoProviderTypeString);
        }
        public static IDataStore CreateProviderFromString(string connectionString, AutoCreateOption autoCreateOption, out IDisposable[] objectsToDisposeOnDisconnect) {
            IDbConnection connection = CreateConnection(connectionString);
            objectsToDisposeOnDisconnect = new IDisposable[] { connection };
            return CreateProviderFromConnection(connection, autoCreateOption);
        }
        public static IDataStore CreateProviderFromConnection(IDbConnection connection, AutoCreateOption autoCreateOption) {
            return new AseClientConnectionProvider(connection, autoCreateOption);
        }

        private const string AseAssemblyName = "AdoNetCore.AseClient";
        private const string AseConnectionShortName = "AseConnection";
        private const string AseExceptionName = "AdoNetCore.AseClient.AseException";
        private const string AseParameterName = "AdoNetCore.AseClient.AseParameter";
        private const string AseDbTypeName = "AdoNetCore.AseClient.AseDbType";
        private const string AseDbTypeShortName = "AseDbType";
        private const string AseCommandBuilderName = "AdoNetCore.AseClient.AseCommandBuilder";
        private const string AseConnectionFullName = "AdoNetCore.AseClient.AseConnection";

        static AseClientConnectionProvider() {
            RegisterDataStoreProvider(XpoProviderTypeString, new DataStoreCreationFromStringDelegate(CreateProviderFromString));
            RegisterDataStoreProvider(AseConnectionShortName, new DataStoreCreationFromConnectionDelegate(CreateProviderFromConnection));
        }
        public static void Register() { }

        public AseClientConnectionProvider(IDbConnection connection, AutoCreateOption autoCreateOption)
            : base(connection, autoCreateOption) {
        }
        protected override string GetSqlCreateColumnTypeForBoolean(DBTable table, DBColumn column) {
            return "bit";
        }
        protected override string GetSqlCreateColumnTypeForByte(DBTable table, DBColumn column) {
            return "tinyint";
        }
        protected override string GetSqlCreateColumnTypeForSByte(DBTable table, DBColumn column) {
            return "numeric(3,0)";
        }
        protected override string GetSqlCreateColumnTypeForChar(DBTable table, DBColumn column) {
            return "unichar(1)";
        }
        protected override string GetSqlCreateColumnTypeForDecimal(DBTable table, DBColumn column) {
            return "money";
        }
        protected override string GetSqlCreateColumnTypeForDouble(DBTable table, DBColumn column) {
            return "double precision";
        }
        protected override string GetSqlCreateColumnTypeForSingle(DBTable table, DBColumn column) {
            return "real";
        }
        protected override string GetSqlCreateColumnTypeForInt32(DBTable table, DBColumn column) {
            return "integer";
        }
        protected override string GetSqlCreateColumnTypeForUInt32(DBTable table, DBColumn column) {
            return "unsigned integer";
        }
        protected override string GetSqlCreateColumnTypeForInt16(DBTable table, DBColumn column) {
            return "smallint";
        }
        protected override string GetSqlCreateColumnTypeForUInt16(DBTable table, DBColumn column) {
            return "unsigned smallint";
        }
        protected override string GetSqlCreateColumnTypeForInt64(DBTable table, DBColumn column) {
            return "bigint";
        }
        protected override string GetSqlCreateColumnTypeForUInt64(DBTable table, DBColumn column) {
            return "unsigned bigint";
        }
        public const int MaximumStringSize = 800;
        protected override string GetSqlCreateColumnTypeForString(DBTable table, DBColumn column) {
            if(column.Size > 0 && column.Size <= MaximumStringSize)
                return "univarchar(" + column.Size.ToString(CultureInfo.InvariantCulture) + ')';
            else
                return "unitext";
        }
        protected override string GetSqlCreateColumnTypeForDateTime(DBTable table, DBColumn column) {
            return "datetime";
        }
        protected override string GetSqlCreateColumnTypeForGuid(DBTable table, DBColumn column) {
            return "char(36)";
        }
        protected override string GetSqlCreateColumnTypeForByteArray(DBTable table, DBColumn column) {
            return "image";
        }
        const int MaxVarLength = 16384;
        string GetSqlCreateColumnTypeSp(DBTable table, DBColumn column) {
            string type = GetSqlCreateColumnType(table, column);
            switch(type) {
                case "image":
                    return string.Format("varbinary({0})", MaxVarLength);
                case "unitext":
                    return string.Format("varchar({0})", MaxVarLength);
            }
            return type;
        }
        public override string GetSqlCreateColumnFullAttributes(DBTable table, DBColumn column) {
            return null;
        }
        public override string GetSqlCreateColumnFullAttributes(DBTable table, DBColumn column, bool forTableCreate) {
            string result = GetSqlCreateColumnFullAttributes(table, column);
            if(!string.IsNullOrEmpty(result)) {
                return result;
            }
            result = GetSqlCreateColumnType(table, column);
            if(!column.IsIdentity) {
                if(!string.IsNullOrEmpty(column.DbDefaultValue)) {
                    result += string.Concat(" DEFAULT ", column.DbDefaultValue);
                } else {
                    if(column.DefaultValue != null && column.DefaultValue != System.DBNull.Value) {
                        string formattedDefaultValue = FormatConstant(column.DefaultValue);
                        result += string.Concat(" DEFAULT ", formattedDefaultValue);
                    }
                }
            }
            if(column.IsKey || !column.IsNullable || column.ColumnType == DBColumnType.Boolean) {
                if(column.IsIdentity && (column.ColumnType == DBColumnType.Int32 || column.ColumnType == DBColumnType.Int64) && IsSingleColumnPKColumn(table, column)) {
                    result += " IDENTITY";
                } else {
                    result += " NOT NULL";
                }
            } else {
                result += " NULL";
            }
            return result;
        }
        protected override object ConvertToDbParameter(object clientValue, TypeCode clientValueTypeCode) {
            switch(clientValueTypeCode) {
                case TypeCode.Object:
                    if(clientValue is Guid) {
                        return clientValue.ToString();
                    }
                    break;
                case TypeCode.SByte:
                    return (Int16)(SByte)clientValue;
                case TypeCode.UInt16:
                    return (Int32)(UInt16)clientValue;
                case TypeCode.UInt32:
                    return (Int64)(UInt32)clientValue;
                case TypeCode.UInt64:
                    return (Decimal)(UInt64)clientValue;
                case TypeCode.Int64:
                    return (Decimal)(Int64)clientValue;
            }
            return base.ConvertToDbParameter(clientValue, clientValueTypeCode);
        }
        protected override Int64 GetIdentity(Query sql) {
            object value = GetScalar(new Query(sql.Sql + "\nselect @@Identity", sql.Parameters, sql.ParametersNames));
            return (value as IConvertible).ToInt64(CultureInfo.InvariantCulture);
        }
        protected override IDbConnection CreateConnection() {
            return ConnectionHelper.GetConnection(ConnectionString);
        }
        public static IDbConnection CreateConnection(string connectionString) {
            string typeName = AseConnectionFullName;
            var connection = ReflectConnectionHelper.GetConnection(AseAssemblyName, typeName, true);
            connection.ConnectionString = connectionString;
            return connection;
        }
        protected override void CreateDataBase() {
            const int CannotOpenDatabaseError = 911;
            try {
                ConnectionStringParser parser = new ConnectionStringParser(ConnectionString);
                parser.RemovePartByName("Pooling");
                string connectString = parser.GetConnectionString() + ";Pooling=false";
                using(IDbConnection conn = ConnectionHelper.GetConnection(connectString)) {
                    conn.Open();
                }
            } catch(Exception e) {
                object o;
                if(ConnectionHelper.TryGetExceptionProperty(e, "Errors", out o)
                    && ((ICollection)o).Count > 0
                    && ((int)ReflectConnectionHelper.GetPropertyValue(ReflectConnectionHelper.GetCollectionFirstItem(((ICollection)o)), "MessageNumber")) == CannotOpenDatabaseError
                    && CanCreateDatabase) {
                    ConnectionStringParser helper = new ConnectionStringParser(ConnectionString);
                    string dbName = helper.GetPartByName("initial catalog");
                    helper.RemovePartByName("initial catalog");
                    string connectToServer = helper.GetConnectionString();
                    using(IDbConnection conn = ConnectionHelper.GetConnection(connectToServer)) {
                        conn.Open();
                        using(IDbCommand c = conn.CreateCommand()) {
                            c.CommandText = "Create Database " + dbName;
                            c.ExecuteNonQuery();
                            c.CommandText = "exec master.dbo.sp_dboption " + dbName + ", 'ddl in tran', true";
                            c.ExecuteNonQuery();
                        }
                    }
                } else
                    throw new UnableToOpenDatabaseException(ConnectionString, e);//XpoDefault.ConnectionStringRemovePassword(ConnectionString), e);
            }
        }
        protected override bool IsConnectionBroken(Exception e) {
            object o;
            if(ConnectionHelper.TryGetExceptionProperty(e, "Errors", out o)
                && ((ICollection)o).Count > 0) {
                object error = ReflectConnectionHelper.GetCollectionFirstItem((ICollection)o);
                int messageNumber = (int)ReflectConnectionHelper.GetPropertyValue(error, "MessageNumber");
                if(messageNumber == 30046) {
                    Connection.Close();
                    return true;
                }
            }
            return base.IsConnectionBroken(e);
        }
        protected override Exception WrapException(Exception e, IDbCommand query) {
            object o;
            if(ConnectionHelper.TryGetExceptionProperty(e, "Errors", out o)
                && ((ICollection)o).Count > 0) {
                object error = ReflectConnectionHelper.GetCollectionFirstItem((ICollection)o);
                int messageNumber = (int)ReflectConnectionHelper.GetPropertyValue(error, "MessageNumber");
                if(messageNumber == 208 || messageNumber == 207) {
                    return new SchemaCorrectionNeededException((string)ReflectConnectionHelper.GetPropertyValue(error, "Message"), e);
                }
                if(messageNumber == 2601 || messageNumber == 547) {
                    return new ConstraintViolationException(query.CommandText, GetParametersString(query), e);
                }
                if(messageNumber == 226) {
                    string msg = "Command is not allowed within a multi-statement transaction.";
                    return new DataException(msg, e);
                }
            }
            return base.WrapException(e, query);
        }
        SelectStatementResult GetDataForTables(ICollection tables, Func<DBTable, bool> filter, string queryText) {
            QueryParameterCollection parameters = new QueryParameterCollection();
            StringCollection inList = new StringCollection();
            int i = 0;
            foreach(DBTable table in tables) {
                if(filter == null || filter(table)) {
                    parameters.Add(new OperandValue(ComposeSafeTableName(table.Name)));
                    inList.Add("@p" + i.ToString(CultureInfo.InvariantCulture));
                    ++i;
                }
            }
            if(inList.Count == 0)
                return new SelectStatementResult();
            return SelectData(new Query(string.Format(CultureInfo.InvariantCulture, queryText, StringListHelper.DelimitedText(inList, ",")), parameters, inList));
        }
        DBColumnType GetTypeFromNumber(byte type, byte prec, int length, short userType, byte charsize, out int len) {
            len = 0;
            switch(type) {
                case 38:
                    switch(length) {
                        case 1:
                            return DBColumnType.Byte;
                        case 2:
                            return DBColumnType.Int16;
                        case 4:
                            return DBColumnType.Int32;
                        case 8:
                            return DBColumnType.Int64;
                        default:
                            return DBColumnType.Unknown;
                    }
                case 68:
                    switch(length) {
                        case 1:
                            return DBColumnType.Byte;
                        case 2:
                            return DBColumnType.UInt16;
                        case 4:
                            return DBColumnType.UInt32;
                        case 8:
                            return DBColumnType.UInt64;
                        default:
                            return DBColumnType.Unknown;
                    }
                case 56:
                    return DBColumnType.Int32;
                case 66:
                    return DBColumnType.UInt32;
                case 52:
                    return DBColumnType.Int16;
                case 65:
                    return DBColumnType.UInt16;
                case 50:
                    return DBColumnType.Boolean;
                case 39:
                case 35:
                    if(userType == 2)
                        len = length;
                    else
                        len = length / charsize;
                    return DBColumnType.String;
                case 174:
                case 155:
                    len = length / 2;
                    return DBColumnType.String;
                case 34:
                case 45:
                    return DBColumnType.ByteArray;
                case 111:
                case 61:
                    return DBColumnType.DateTime;
                case 109:
                    return DBColumnType.Double;
                case 110:
                case 60:
                    return DBColumnType.Decimal;
                case 108:
                case 63:
                    if(prec <= 3)
                        return DBColumnType.SByte;
                    if(prec <= 5)
                        return DBColumnType.Int16;
                    if(prec <= 10)
                        return DBColumnType.Int32;
                    return DBColumnType.Int64;
            }
            return DBColumnType.Unknown;
        }
        void GetColumns(DBTable table) {
            foreach(SelectStatementResultRow row in SelectData(new Query("select c.name, c.type, c.prec, c.length, c.usertype, @@ncharsize, c.status, dflt.name defaultValueName " +
                "from syscolumns c " +
                "left join sysobjects t on c.id = t.id " +
                "left join sysobjects dflt on c.cdefault=dflt.id and dflt.type='D' " +
                "where t.name = @p1", 
                new QueryParameterCollection(new OperandValue(ComposeSafeTableName(table.Name))), new string[] { "@p1" })).Rows) {
                int len;
                DBColumnType type = GetTypeFromNumber((byte)row.Values[1], row.Values[2] is DBNull ? (byte)0 : (byte)row.Values[2], (int)row.Values[3], (short)row.Values[4], (byte)row.Values[5], out len);
                bool isNullable = (Convert.ToInt32(row.Values[6]) & 0x08) != 0;

                string dbDefaultValue = null;
                object defaultValue = null;
                string dbDefaultValueName = (row.Values[7] as string);
                if(!string.IsNullOrEmpty(dbDefaultValueName)) {
                    dbDefaultValue = GetColumnDefaultValueSqlExpression(dbDefaultValueName);
                    if(!string.IsNullOrEmpty(dbDefaultValue)) {
                        if(dbDefaultValue == "''" && (type == DBColumnType.Char || type == DBColumnType.String)) {
                            defaultValue = "";
                        } else {
                            string scalarQuery = string.Concat("select ", dbDefaultValue);
                            try {
                                defaultValue = FixDBNullScalar(GetScalar(new Query(scalarQuery)));
                            } catch { }
                        }
                    }
                    if(defaultValue != null) {
                        ReformatReadValueArgs refmtArgs = new ReformatReadValueArgs(DBColumn.GetType(type));
                        refmtArgs.AttachValueReadFromDb(defaultValue);
                        defaultValue = ReformatReadValue(defaultValue, refmtArgs);
                    }
                }

                DBColumn column = new DBColumn((string)row.Values[0], false, String.Empty, len, type, isNullable, defaultValue);
                column.IsIdentity = (Convert.ToInt32(row.Values[6]) & 128) == 128;
                column.DbDefaultValue = dbDefaultValue;
                table.AddColumn(column);
            }
        }
        string GetColumnDefaultValueSqlExpression(string dbDefaultValueName) {
            Query query = new Query("sp_helptext @p1", new QueryParameterCollection(new ConstantValue(dbDefaultValueName)), new string[] { "p1" });
            using(IDbCommand command = CreateCommand(query)) {
                SelectStatementResult[] results = InternalGetData(command, null, 0, 0, false);
                StringBuilder sb = new StringBuilder();
                foreach(SelectStatementResultRow row in results[1].Rows) {
                    sb.Append(row.Values[0].ToString());
                }
                string sqlExpr = sb.ToString();
                if(sqlExpr.StartsWith("DEFAULT ")) {
                    return sqlExpr.Remove(0, 8).Trim();
                } else {
                    return sqlExpr.Trim();
                }
            }
        }
        void GetPrimaryKey(DBTable table) {
            SelectStatementResult data = SelectData(new Query("select index_col(o.name, i.indid, 1), i.keycnt from sysindexes i join sysobjects o on o.id = i.id where i.status & 2048 <> 0 and o.name = @p1", new QueryParameterCollection(new OperandValue(ComposeSafeTableName(table.Name))), new string[] { "@p1" }));
            if(data.Rows.Length > 0) {
                if((short)data.Rows[0].Values[1] != 1)
                    throw new NotImplementedException(MulticolumnIndexesAreNotSupported); //todo
                StringCollection cols = new StringCollection();
                cols.Add((string)data.Rows[0].Values[0]);
                foreach(string columnName in cols) {
                    DBColumn column = table.GetColumn(columnName);
                    if(column != null)
                        column.IsKey = true;
                }
                table.PrimaryKey = new DBPrimaryKey(cols);
            }
        }
        public override void CreateIndex(DBTable table, DBIndex index) {
            if(table.Name != "XPObjectType")
                base.CreateIndex(table, index);
        }
        void GetIndexes(DBTable table) {
            SelectStatementResult data = SelectData(new Query("select index_col(o.name, i.indid, 1), i.keycnt, (i.status & 2) from sysindexes i join sysobjects o on o.id = i.id where o.name = @p1 and i.keycnt > 1 and i.status & 2048 = 0", new QueryParameterCollection(new OperandValue(ComposeSafeTableName(table.Name))), new string[] { "@p1" }));
            foreach(SelectStatementResultRow row in data.Rows) {
                if((short)row.Values[1] != 2)
                    throw new NotImplementedException(MulticolumnIndexesAreNotSupported);
                StringCollection cols = new StringCollection();
                cols.Add((string)row.Values[0]);
                table.Indexes.Add(new DBIndex(cols, Convert.ToInt32(row.Values[2]) == 2));
            }
        }
        void GetForeignKeys(DBTable table) {
            SelectStatementResult data = SelectData(new Query(
@"select f.keycnt, fc.name, pc.name, r.name from sysreferences f
join sysobjects o on o.id = f.tableid
join sysobjects r on r.id = f.reftabid
join syscolumns fc on f.fokey1 = fc.colid and fc.id = o.id
join syscolumns pc on f.refkey1 = pc.colid and pc.id = r.id
where o.name = @p1", new QueryParameterCollection(new OperandValue(ComposeSafeTableName(table.Name))), new string[] { "@p1" }));
            foreach(SelectStatementResultRow row in data.Rows) {
                if((short)row.Values[0] != 1)
                    throw new NotImplementedException(MulticolumnIndexesAreNotSupported);
                StringCollection pkc = new StringCollection();
                StringCollection fkc = new StringCollection();
                pkc.Add((string)row.Values[1]);
                fkc.Add((string)row.Values[2]);
                table.ForeignKeys.Add(new DBForeignKey(pkc, (string)row.Values[3], fkc));
            }
        }
        public override void GetTableSchema(DBTable table, bool checkIndexes, bool checkForeignKeys) {
            GetColumns(table);
            GetPrimaryKey(table);
            if(checkIndexes)
                GetIndexes(table);
            if(checkForeignKeys)
                GetForeignKeys(table);
        }
        public override ICollection CollectTablesToCreate(ICollection tables) {
            Hashtable dbTables = new Hashtable();
            foreach(SelectStatementResultRow row in GetDataForTables(tables, null, "select name,type from sysobjects where name in ({0}) and type in ('U', 'V')").Rows)
                dbTables.Add(row.Values[0], ((string)row.Values[1]).Trim() == "V");
            ArrayList list = new ArrayList();
            foreach(DBTable table in tables) {
                object o = dbTables[ComposeSafeTableName(table.Name)];
                if(o == null)
                    list.Add(table);
                else
                    table.IsView = (bool)o;
            }
            return list;
        }
        protected override int GetSafeNameTableMaxLength() {
            return 28;
        }
        public override string FormatTable(string schema, string tableName) {
            return string.Format(CultureInfo.InvariantCulture, "[{0}]", tableName);
        }
        public override string FormatTable(string schema, string tableName, string tableAlias) {
            return string.Format(CultureInfo.InvariantCulture, "[{0}] {1}", tableName, tableAlias);
        }
        public override string FormatColumn(string columnName) {
            return string.Format(CultureInfo.InvariantCulture, "[{0}]", columnName);
        }
        public override string FormatColumn(string columnName, string tableAlias) {
            return string.Format(CultureInfo.InvariantCulture, "{1}.[{0}]", columnName, tableAlias);
        }
        public override string FormatSelect(string selectedPropertiesSql, string fromSql, string whereSql, string orderBySql, string groupBySql, string havingSql, int topSelectedRecords) {
            string modificatorsSql = string.Format(CultureInfo.InvariantCulture, (topSelectedRecords != 0) ? "top {0} " : string.Empty, topSelectedRecords);
            string expandedWhereSql = whereSql != null ? string.Format(CultureInfo.InvariantCulture, "{0}where {1}", Environment.NewLine, whereSql) : string.Empty;
            string expandedOrderBySql = orderBySql != null ? string.Format(CultureInfo.InvariantCulture, "{0}order by {1}", Environment.NewLine, orderBySql) : string.Empty;
            string expandedHavingSql = havingSql != null ? string.Format(CultureInfo.InvariantCulture, "{0}having {1}", Environment.NewLine, havingSql) : string.Empty;
            string expandedGroupBySql = groupBySql != null ? string.Format(CultureInfo.InvariantCulture, "{0}group by {1}", Environment.NewLine, groupBySql) : string.Empty;
            if(topSelectedRecords == 0)
                return string.Format(CultureInfo.InvariantCulture, "select {0}{1} from {2}{3}{4}{5}{6}", modificatorsSql, selectedPropertiesSql, fromSql, expandedWhereSql, expandedGroupBySql, expandedHavingSql, expandedOrderBySql);
            else
                return string.Format(CultureInfo.InvariantCulture, "set rowcount {0} select {1} from {2}{3}{4}{5}{6} set rowcount 0", topSelectedRecords, selectedPropertiesSql, fromSql, expandedWhereSql, expandedGroupBySql, expandedHavingSql, expandedOrderBySql);
        }
        public override string FormatInsertDefaultValues(string tableName) {
            return string.Format(CultureInfo.InvariantCulture, "insert into {0} values()", tableName);
        }
        public override string FormatInsert(string tableName, string fields, string values) {
            return string.Format(CultureInfo.InvariantCulture, "insert into {0}({1})values({2})",
                tableName, fields, values);
        }
        public override string FormatUpdate(string tableName, string sets, string whereClause) {
            return string.Format(CultureInfo.InvariantCulture, "update {0} set {1} where {2}",
                tableName, sets, whereClause);
        }
        public override string FormatDelete(string tableName, string whereClause) {
            return string.Format(CultureInfo.InvariantCulture, "delete from {0} where {1}", tableName, whereClause);
        }
        public override string FormatBinary(BinaryOperatorType operatorType, string leftOperand, string rightOperand) {
            switch(operatorType) {
                case BinaryOperatorType.Modulo:
                    return string.Format(CultureInfo.InvariantCulture, "{0} % {1}", leftOperand, rightOperand);
                case BinaryOperatorType.BitwiseAnd:
                    return string.Format(CultureInfo.InvariantCulture, "({0} & {1})", leftOperand, rightOperand);
                case BinaryOperatorType.BitwiseOr:
                    return string.Format(CultureInfo.InvariantCulture, "({0} | {1})", leftOperand, rightOperand);
                case BinaryOperatorType.BitwiseXor:
                    return string.Format(CultureInfo.InvariantCulture, "({0} ^ {1})", leftOperand, rightOperand);
                default:
                    return base.FormatBinary(operatorType, leftOperand, rightOperand);
            }
        }
        public override string FormatFunction(FunctionOperatorType operatorType, params string[] operands) {
            switch(operatorType) {
                case FunctionOperatorType.Acos:
                    return string.Format(CultureInfo.InvariantCulture, "acos({0})", operands[0]);
                case FunctionOperatorType.Asin:
                    return string.Format(CultureInfo.InvariantCulture, "asin({0})", operands[0]);
                case FunctionOperatorType.Atn:
                    return string.Format(CultureInfo.InvariantCulture, "atan({0})", operands[0]);
                case FunctionOperatorType.Atn2:
                    return string.Format(CultureInfo.InvariantCulture, "(case when {0} = 0 then (case when {1} >= 0 then 0 else atan(1) * 4 end) else 2 * atan({0} / (sqrt({1} * {1} + {0} * {0}) + {1})) end)", operands[0], operands[1]);
                case FunctionOperatorType.Cosh:
                    return string.Format(CultureInfo.InvariantCulture, "((exp({0}) + exp({0} * -1)) / 2)", operands[0]);
                case FunctionOperatorType.Sinh:
                    return string.Format(CultureInfo.InvariantCulture, "((exp({0}) - exp({0} * -1)) / 2)", operands[0]);
                case FunctionOperatorType.Tanh:
                    return string.Format(CultureInfo.InvariantCulture, "((exp({0} * 2) - 1) / (exp({0} * 2) + 1))", operands[0]);
                case FunctionOperatorType.Log:
                    return FnLog(operands);
                case FunctionOperatorType.Log10:
                    return string.Format(CultureInfo.InvariantCulture, "log10({0})", operands[0]);
                case FunctionOperatorType.Round:
                    switch(operands.Length) {
                        case 1:
                            return string.Format(CultureInfo.InvariantCulture, "round({0}, 0)", operands[0]);
                        case 2:
                            return string.Format(CultureInfo.InvariantCulture, "round({0}, {1})", operands[0], operands[1]);
                    }
                    goto default;
                case FunctionOperatorType.Sqr:
                    return string.Format(CultureInfo.InvariantCulture, "sqrt({0})", operands[0]);
                case FunctionOperatorType.ToInt:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} AS integer)", operands[0]);
                case FunctionOperatorType.ToLong:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} AS bigint)", operands[0]);
                case FunctionOperatorType.ToFloat:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} AS real)", operands[0]);
                case FunctionOperatorType.ToDouble:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} AS double precision)", operands[0]);
                case FunctionOperatorType.ToDecimal:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} AS money)", operands[0]);
                case FunctionOperatorType.BigMul:
                    return string.Format(CultureInfo.InvariantCulture, "cast({0} * {1} as bigint)", operands[0], operands[1]);
                case FunctionOperatorType.Max:
                    return string.Format(CultureInfo.InvariantCulture, "(case when {0} > {1} then {0} else {1} end)", operands[0], operands[1]);
                case FunctionOperatorType.Min:
                    return string.Format(CultureInfo.InvariantCulture, "(case when {0} < {1} then {0} else {1} end)", operands[0], operands[1]);
                case FunctionOperatorType.Rnd:
                    return "Rand()";
                case FunctionOperatorType.CharIndex:
                    return FnCharIndex(operands);
                case FunctionOperatorType.PadLeft:
                    return FnLpad(operands);
                case FunctionOperatorType.PadRight:
                    return FnRpad(operands);
                case FunctionOperatorType.Remove:
                    return FnRemove(operands);
                case FunctionOperatorType.GetMilliSecond:
                    return string.Format(CultureInfo.InvariantCulture, "datepart(ms, {0})", operands[0]);
                case FunctionOperatorType.AddTicks:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, (cast({1} as bigint) / 10000) % 86400000, dateadd(day, (cast({1} as bigint) / 10000) / 86400000, {0}))", operands[0], operands[1]);
                case FunctionOperatorType.AddMilliSeconds:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, {1}, {0})", operands[0], operands[1]);
                case FunctionOperatorType.AddTimeSpan:
                case FunctionOperatorType.AddSeconds:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, cast((cast({1} as numeric(38,19)) * 1000) as bigint) % 86400000, dateadd(day, cast((cast({1} as numeric(38,19)) * 1000) / 86400000 as bigint), {0}))", operands[0], operands[1]);
                case FunctionOperatorType.AddMinutes:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, cast((cast({1} as numeric(38,19)) * 60000) as bigint) % 86400000, dateadd(day, cast((cast({1} as numeric(38,19)) * 60000) / 86400000 as bigint), {0}))", operands[0], operands[1]);
                case FunctionOperatorType.AddHours:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, cast((cast({1} as numeric(38,19)) * 3600000) as bigint) % 86400000, dateadd(day, cast((cast({1} as numeric(38,19)) * 3600000) / 86400000 as bigint), {0}))", operands[0], operands[1]);
                case FunctionOperatorType.AddDays:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(ms, cast((cast({1} as numeric(38,19)) * 86400000) as bigint) % 86400000, dateadd(day, cast((cast({1} as numeric(38,19)) * 86400000) / 86400000 as bigint), {0}))", operands[0], operands[1]);
                case FunctionOperatorType.AddMonths:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(month, {1}, {0})", operands[0], operands[1]);
                case FunctionOperatorType.AddYears:
                    return string.Format(CultureInfo.InvariantCulture, "dateadd(year, {1}, {0})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffYear:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(yy, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffMonth:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(mm, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffDay:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(dd, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffHour:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(hh, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffMinute:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(mi, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffSecond:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(ss, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffMilliSecond:
                    return string.Format(CultureInfo.InvariantCulture, "datediff(ms, {0}, {1})", operands[0], operands[1]);
                case FunctionOperatorType.DateDiffTick:
                    return string.Format(CultureInfo.InvariantCulture, "(datediff(ms, {0}, {1}) * 10000)", operands[0], operands[1]);
                case FunctionOperatorType.Now:
                    return "getdate()";
                case FunctionOperatorType.UtcNow:
                    return "getutcdate()";
                case FunctionOperatorType.Today:
                    return "cast(cast(getdate() as date) as datetime)";
                case FunctionOperatorType.GetDate:
                    return string.Format(CultureInfo.InvariantCulture, "cast(cast({0} as date) as datetime)", operands[0]);
                case FunctionOperatorType.IsNull:
                    switch(operands.Length) {
                        case 1:
                            return string.Format(CultureInfo.InvariantCulture, "(({0}) is null)", operands[0]);
                        case 2:
                            return string.Format(CultureInfo.InvariantCulture, "isnull({0}, {1})", operands[0], operands[1]);
                    }
                    goto default;
                case FunctionOperatorType.IsNullOrEmpty:
                    return string.Format(CultureInfo.InvariantCulture, "(({0}) is null or ({0}) = '')", operands[0]);
                case FunctionOperatorType.Contains:
                    return string.Format(CultureInfo.InvariantCulture, "(CharIndex({1}, {0}) > 0)", operands[0], operands[1]);
                case FunctionOperatorType.EndsWith:
                    return string.Format(CultureInfo.InvariantCulture, "(Right({0}, Len({1})) = ({1}))", operands[0], operands[1]);
                default:
                    return base.FormatFunction(operatorType, operands);
            }
        }
        readonly static char[] achtungChars = new char[] { '_', '%', '[', ']' };
        public override string FormatFunction(ProcessParameter processParameter, FunctionOperatorType operatorType, params object[] operands) {
            switch(operatorType) {
                case FunctionOperatorType.StartsWith:
                    object secondOperand = operands[1];
                    if(secondOperand is OperandValue && ((OperandValue)secondOperand).Value is string) {
                        string operandString = (string)((OperandValue)secondOperand).Value;
                        int likeIndex = operandString.IndexOfAny(achtungChars);
                        if(likeIndex < 0) {
                            return string.Format(CultureInfo.InvariantCulture, "({0} like {1})", processParameter(operands[0]), processParameter(new ConstantValue(operandString + "%")));
                        } else if(likeIndex > 0) {
                            return string.Format(CultureInfo.InvariantCulture, "(({0} like {2}) And (CharIndex({1}, {0}) = 1))", processParameter(operands[0]), processParameter(secondOperand), processParameter(new ConstantValue(operandString.Substring(0, likeIndex) + "%")));
                        }
                    }
                    return string.Format(CultureInfo.InvariantCulture, "(CharIndex({1}, {0}) = 1)", processParameter(operands[0]), processParameter(operands[1]));
                default:
                    return base.FormatFunction(processParameter, operatorType, operands);
            }
        }
        string FnRemove(string[] operands) {
            switch(operands.Length) {
                case 2:
                    return string.Format(CultureInfo.InvariantCulture, "substring({0}, 1, {1})", operands[0], operands[1]);
                case 3:
                    return string.Format(CultureInfo.InvariantCulture, "stuff({0}, {1} + 1, {2}, null)", operands[0], operands[1], operands[2]);
                default:
                    throw new NotSupportedException();
            }
        }
        string FnRpad(string[] operands) {
            switch(operands.Length) {
                case 2:
                    return string.Format(CultureInfo.InvariantCulture, "({0} + replicate(' ', {1} - char_length({0})))", operands[0], operands[1]);
                case 3:
                    return string.Format(CultureInfo.InvariantCulture, "({0} + replicate({2}, {1} - char_length({0})))", operands[0], operands[1], operands[2]);
                default:
                    throw new NotSupportedException();
            }
        }
        string FnLpad(string[] operands) {
            switch(operands.Length) {
                case 2:
                    return string.Format(CultureInfo.InvariantCulture, "(replicate(' ', {1} - char_length({0})) + {0})", operands[0], operands[1]);
                case 3:
                    return string.Format(CultureInfo.InvariantCulture, "(replicate({2}, {1} - char_length({0})) + {0})", operands[0], operands[1], operands[2]);
                default:
                    throw new NotSupportedException();
            }
        }
        string FnLog(string[] operands) {
            switch(operands.Length) {
                case 1:
                    return string.Format(CultureInfo.InvariantCulture, "log({0})", operands[0]);
                case 2:
                    return string.Format(CultureInfo.InvariantCulture, "(log({0}) / log({1}))", operands[0], operands[1]);
                default:
                    throw new NotSupportedException();
            }
        }
        string FnCharIndex(string[] operands) {
            switch(operands.Length) {
                case 2:
                    return string.Format(CultureInfo.InvariantCulture, "(charindex({0}, {1}) - 1)", operands[0], operands[1]);
                case 3:
                    return string.Format(CultureInfo.InvariantCulture, "(case when charindex({0}, substring({1}, {2} + 1, char_length({1}) - {2})) > 0 then charindex({0}, substring({1}, {2} + 1, char_length({1}) - {2})) + {2} - 1 else -1 end)", operands[0], operands[1], operands[2]);
                case 4:
                    return string.Format(CultureInfo.InvariantCulture, "(case when charindex({0}, substring({1}, {2} + 1, {3} - {2})) > 0 then charindex({0}, substring({1}, {2} + 1, {3} - {2})) + {2} - 1 else -1 end)", operands[0], operands[1], operands[2], operands[3]);
                default:
                    throw new NotSupportedException();
            }
        }
        public override string GetParameterName(OperandValue parameter, int index, ref bool createParameter) {
            object value = parameter.Value;
            createParameter = false;
            if(parameter is ConstantValue && value != null) {
                switch(Type.GetTypeCode(value.GetType())) {
                    case TypeCode.Int32:
                        return ((int)value).ToString(CultureInfo.InvariantCulture);
                    case TypeCode.Boolean:
                        return (bool)value ? "1" : "0";
                    case TypeCode.String:
                        return FormatString(value);
                }
            }
            createParameter = true;
            return "@p" + index.ToString(CultureInfo.InvariantCulture);
        }
        string FormatString(object value) {
            return "'" + ((string)value).Replace("'", "''") + "'";
        }
        object aseDbTypeDecimal;
        object aseDbTypeImage;
        object aseDbTypeUnitext;
        object aseDbTypeUniVarChar;
        SetPropertyValueDelegate setAseDbParameterDbType;
        protected override IDataParameter CreateParameter(IDbCommand command, object value, string name) {
            IDataParameter param = CreateParameter(command);
            param.Value = value;
            param.ParameterName = name;
            if(value is decimal) {
                if(setAseDbParameterDbType == null)
                    InitAseDbTypeVars();
                setAseDbParameterDbType(param, aseDbTypeDecimal);
            }
            if(value is byte[]) {
                if(setAseDbParameterDbType == null)
                    InitAseDbTypeVars();
                setAseDbParameterDbType(param, aseDbTypeImage);
            }
            if(value is string) {
                if(setAseDbParameterDbType == null)
                    InitAseDbTypeVars();
                if(((string)value).Length > MaximumStringSize)
                    setAseDbParameterDbType(param, aseDbTypeUnitext);
                else
                    setAseDbParameterDbType(param, aseDbTypeUniVarChar);
            }
            return param;
        }

        void InitAseDbTypeVars() {
            Type aseDbParameterType = ConnectionHelper.GetType(AseParameterName);
            Type aseDbType = ConnectionHelper.GetType(AseDbTypeName);
            aseDbTypeDecimal = Enum.Parse(aseDbType, "Decimal", false);
            aseDbTypeImage = Enum.Parse(aseDbType, "Image", false);
            aseDbTypeUnitext = Enum.Parse(aseDbType, "Unitext", false);
            aseDbTypeUniVarChar = Enum.Parse(aseDbType, "UniVarChar", false);
            setAseDbParameterDbType = ReflectConnectionHelper.CreateSetPropertyDelegate(aseDbParameterType, AseDbTypeShortName);
        }
        public override string FormatConstraint(string constraintName) {
            return string.Format(CultureInfo.InvariantCulture, "[{0}]", constraintName);
        }
        protected string FormatConstant(object value) {
            TypeCode tc = DXTypeExtensions.GetTypeCode(value.GetType());
            switch(tc) {
                case DXTypeExtensions.TypeCodeDBNull:
                case TypeCode.Empty:
                    return "NULL";
                case TypeCode.Boolean:
                    return ((bool)value) ? "1" : "0";
                case TypeCode.Char:
                    if(value is char && Convert.ToInt32(value) < 32) {
                        return string.Concat("char(", Convert.ToInt32(value).ToString(CultureInfo.InvariantCulture), ")");
                    } else {
                        return "'" + (char)value + "'";
                    }
                case TypeCode.DateTime:
                    DateTime datetimeValue = (DateTime)value;
                    string dateTimeFormatPattern = "yyyy-MM-dd HH:mm:ss";
                    return string.Format("cast('{0}' as datetime)", datetimeValue.ToString(dateTimeFormatPattern, CultureInfo.InvariantCulture));
                case TypeCode.String:
                    return FormatString(value);
                case TypeCode.Decimal:
                    return FixNonFixedText(((Decimal)value).ToString(CultureInfo.InvariantCulture));
                case TypeCode.Double:
                    return FixNonFixedText(((Double)value).ToString("r", CultureInfo.InvariantCulture));
                case TypeCode.Single:
                    return FixNonFixedText(((Single)value).ToString("r", CultureInfo.InvariantCulture));
                case TypeCode.Byte:
                case TypeCode.SByte:
                case TypeCode.Int16:
                case TypeCode.UInt16:
                case TypeCode.Int32:
                case TypeCode.UInt32:
                case TypeCode.Int64:
                    return Convert.ToInt64(value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.UInt64:
                    return Convert.ToUInt64(value).ToString(CultureInfo.InvariantCulture);
                case TypeCode.Object:
                default:
                    if(value is Guid) {
                        return string.Concat("'", ((Guid)value).ToString(), "'");
                    } else if(value is TimeSpan) {
                        return FixNonFixedText(((TimeSpan)value).TotalSeconds.ToString("r", CultureInfo.InvariantCulture));
                    } else {
                        throw new ArgumentException(value.ToString());
                    }
            }
        }
        string FixNonFixedText(string toFix) {
            if(toFix.IndexOfAny(new char[] { '.', 'e', 'E' }) < 0)
                toFix += ".0";
            return toFix;
        }

        void ClearDatabase(IDbCommand command) {
            SelectStatementResult constraints = SelectData(new Query("select o.name, t.name  from sysreferences f join sysobjects o on f.constrid = o.id join sysobjects t on f.tableid = t.id"));
            foreach(SelectStatementResultRow row in constraints.Rows) {
                command.CommandText = "alter table [" + (string)row.Values[1] + "] drop constraint [" + (string)row.Values[0] + "]";
                command.ExecuteNonQuery();
            }

            string[] tables = GetStorageTablesList(false);
            foreach(string table in tables) {
                command.CommandText = "drop table [" + table + "]";
                command.ExecuteNonQuery();
            }
        }
        protected override void ProcessClearDatabase() {
            using(IDbCommand command = CreateCommand()) {
                ClearDatabase(command);
            }
        }
        public override string[] GetStorageTablesList(bool includeViews) {
            SelectStatementResult tables = SelectData(new Query(string.Format("select name from sysobjects where type in ('U'{0})", includeViews ? ", 'V'" : string.Empty)));
            ArrayList result = new ArrayList(tables.Rows.Length);
            foreach(SelectStatementResultRow row in tables.Rows) {
                result.Add(row.Values[0]);
            }
            return (string[])result.ToArray(typeof(string));
        }
        ExecMethodDelegate commandBuilderDeriveParametersHandler;
        protected override void CommandBuilderDeriveParameters(IDbCommand command) {
            if(commandBuilderDeriveParametersHandler == null) {
                commandBuilderDeriveParametersHandler = ReflectConnectionHelper.GetCommandBuilderDeriveParametersDelegate(Connection.GetType().Assembly.FullName, AseCommandBuilderName);
            }
            commandBuilderDeriveParametersHandler(command);
        }
        public override DBStoredProcedure[] GetStoredProcedures() {
            List<DBStoredProcedure> result = new List<DBStoredProcedure>();
            using(var command = Connection.CreateCommand()) {
                command.CommandText = "select * from sysobjects where type = 'P'";
                using(var reader = command.ExecuteReader()) {
                    while(reader.Read()) {
                        DBStoredProcedure curSproc = new DBStoredProcedure();
                        curSproc.Name = reader.GetString(0);
                        result.Add(curSproc);
                    }
                }
            }
            foreach(DBStoredProcedure curSproc in result) {
                List<string> fakeParams = new List<string>();
                using(var command = Connection.CreateCommand()) {
                    command.CommandType = CommandType.StoredProcedure;
                    command.CommandText = curSproc.Name;
                    CommandBuilderDeriveParameters(command);
                    List<DBStoredProcedureArgument> dbArguments = new List<DBStoredProcedureArgument>();
                    foreach(IDataParameter parameter in command.Parameters) {
                        DBStoredProcedureArgumentDirection direction = DBStoredProcedureArgumentDirection.In;
                        if(parameter.Direction == ParameterDirection.InputOutput) {
                            direction = DBStoredProcedureArgumentDirection.InOut;
                        }
                        if(parameter.Direction == ParameterDirection.Output) {
                            direction = DBStoredProcedureArgumentDirection.Out;
                        }
                        DBColumnType columnType = GetColumnType(parameter.DbType, true);
                        dbArguments.Add(new DBStoredProcedureArgument(parameter.ParameterName, columnType, direction));
                        fakeParams.Add("null");
                    }
                    curSproc.Arguments.AddRange(dbArguments);
                }
                using(var command = Connection.CreateCommand()) {
                    command.CommandType = CommandType.Text;
                    command.CommandText = string.Format(
@"set showplan on
set fmtonly on
exec [{0}] {1}
set fmtonly off
set showplan off", ComposeSafeTableName(curSproc.Name), string.Join(", ", fakeParams.ToArray()));
                    using(var reader = command.ExecuteReader()) {
                        DBStoredProcedureResultSet curResultSet = new DBStoredProcedureResultSet();
                        List<DBNameTypePair> dbColumns = new List<DBNameTypePair>();
                        for(int i = 0; i < reader.FieldCount; i++) {
                            dbColumns.Add(new DBNameTypePair(reader.GetName(i), DBColumn.GetColumnType(reader.GetFieldType(i))));
                        }
                        curResultSet.Columns.AddRange(dbColumns);
                        curSproc.ResultSets.Add(curResultSet);
                    }
                }
            }
            return result.ToArray();
        }
    }
}