using System;
using System.Data;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;

namespace PgDbInterface
{
    /// <summary>
    /// Методы для работы с хранимыми процедурами и функциями в БД PostgreSQL
    /// </summary>
    public class BaseSchemaApi
    {
        private readonly DataBaseConnection _connection;

        protected NpgsqlConnection Connection => _connection.Connection;

        /// <summary>
        /// Имя схемы по умолчанию
        /// </summary>
        public string SchemaName { get; }

        /// <summary>
        /// Количество строк, единовременно выбираемых при помощи курсора
        /// </summary>
        public static int CursorFetchSize { get; set; } = 100;

        /// <summary>
        /// Конструктор
        /// </summary>
        /// <param name="connection">Коннект к базе</param>
        /// <param name="schemaName">Имя схемы по умолчанию</param>
        public BaseSchemaApi(DataBaseConnection connection, string schemaName)
        {
            _connection = connection;
            SchemaName = schemaName;
        }


        public void SetParameter(NpgsqlParameterCollection parameters, object value, string name, NpgsqlDbType type,
                                 int size, ParameterDirection direction)
        {
            SetParameter(parameters, value, name, type, direction);
            parameters[name].Size = size;
        }

        public void SetParameter(NpgsqlParameterCollection parameters, object value, string name, NpgsqlDbType type,
                                 ParameterDirection direction)
        {
            bool paramIsNull = false;

            if (value == null)
            {
                paramIsNull = true;
            }
            else
            {
                if (type == NpgsqlDbType.Double && Convert.ToDecimal(value) == decimal.MinValue)
                    paramIsNull = true;
                if (type == NpgsqlDbType.Varchar && Convert.ToString(value) == string.Empty)
                    paramIsNull = true;
                if (type == NpgsqlDbType.Text && Convert.ToString(value) == string.Empty)
                    paramIsNull = true;
            }

            NpgsqlParameter param = parameters.Add(name, type);
            param.Direction = direction;
            param.Value = paramIsNull ? null : value;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <b>Integer</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="int"/></returns>
        protected Task<int?> ExecFuncInt(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncInt(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Integer</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="int"/></returns>
        protected Task<int?> ExecFuncInt(string schemaName, string functionName, DbParameters parameters,
                                         CancellationToken cancellationToken = default)
        {
            return ExecFuncInt(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Integer</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns>Nullable <see cref="int"/></returns>
        public static async Task<int?> ExecFuncInt(string schemaName, string functionName, DbParameters parameters, NpgsqlConnection connection,
                                                   CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Integer, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as int?;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <b>Number</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="double"/></returns>
        protected Task<double?> ExecFuncDouble(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncDouble(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Number</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="double"/></returns>
        protected Task<double?> ExecFuncDouble(string schemaName, string functionName, DbParameters parameters,
                                               CancellationToken cancellationToken = default)
        {
            return ExecFuncDouble(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Number</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns>Nullable <see cref="double"/></returns>
        public static async Task<double?> ExecFuncDouble(string schemaName, string functionName, DbParameters parameters,
                                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Double, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as double?;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <b>Decimal</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="decimal"/></returns>
        protected Task<decimal?> ExecFuncDecimal(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncDecimal(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Decimal</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="decimal"/></returns>
        protected Task<decimal?> ExecFuncDecimal(string schemaName, string functionName, DbParameters parameters,
                                                 CancellationToken cancellationToken = default)
        {
            return ExecFuncDecimal(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Decimal</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns>Nullable <see cref="decimal"/></returns>
        public static async Task<decimal?> ExecFuncDecimal(string schemaName, string functionName, DbParameters parameters,
                                                           NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Numeric, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as decimal?;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="PackageName"/> с типом возвращаемого параметра <b>Date</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="DateTime"/></returns>
        protected Task<DateTime?> ExecFuncDate(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncDate(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Date</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="DateTime"/></returns>
        protected Task<DateTime?> ExecFuncDate(string schemaName, string functionName, DbParameters parameters,
                                               CancellationToken cancellationToken = default)
        {
            return ExecFuncDate(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Date</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns>Nullable <see cref="DateTime"/></returns>
        public static async Task<DateTime?> ExecFuncDate(string schemaName, string functionName, DbParameters parameters,
                                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Timestamp, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as DateTime?;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <b>Varchar2</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task<string> ExecFuncVarchar(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncVarchar(SchemaName, functionName, parameters, cancellationToken);
        }

        protected Task<string> ExecFuncStr(string functionName, DbParameters parameters, NpgsqlConnection connection,
                                           CancellationToken cancellationToken = default)
        {
            return ExecFuncVarchar(SchemaName, functionName, parameters, connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Varchar2</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task<string> ExecFuncVarchar(string schemaName, string functionName, DbParameters parameters,
                                               CancellationToken cancellationToken = default)
        {
            return ExecFuncVarchar(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Varchar2</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="connection">Подключение к базе</param>
        /// <param name="parameters">Входные параметры</param>
        public static async Task<string> ExecFuncVarchar(string schemaName, string functionName, DbParameters parameters,
                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Varchar, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as string;
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <b>Text</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task<string> ExecFuncText(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncText(SchemaName, functionName, parameters, cancellationToken);
        }

        protected Task<string> ExecFuncText(string functionName, DbParameters parameters, NpgsqlConnection connection,
                                            CancellationToken cancellationToken = default)
        {
            return ExecFuncText(SchemaName, functionName, parameters, connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Text</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task<string> ExecFuncText(string schemaName, string functionName, DbParameters parameters,
                                            CancellationToken cancellationToken = default)
        {
            return ExecFuncText(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>Text</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="connection">Подключение к базе</param>
        /// <param name="parameters">Входные параметры</param>
        public static async Task<string> ExecFuncText(string schemaName, string functionName, DbParameters parameters,
                                                      NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Text, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as string;
        }


        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>BYTEA</b>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="double"/></returns>
        public Task<byte[]> ExecFuncBytea(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncBytea(SchemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>BYTEA</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns>Nullable <see cref="double"/></returns>
        public Task<byte[]> ExecFuncBytea(string schemaName, string functionName, DbParameters parameters,
                                          CancellationToken cancellationToken = default)
        {
            return ExecFuncBytea(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете с типом возвращаемого параметра <b>BYTEA</b>
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns>Nullable <see cref="double"/></returns>
        public static async Task<byte[]> ExecFuncBytea(string schemaName, string functionName, DbParameters parameters,
                                                       NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object result = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Bytea, connection, cancellationToken)
                                    .ConfigureAwait(false);
            return result as byte[];
        }


        /// <summary>
        /// Выполняет функцию в пакете <see cref="SchemaName"/> с типом возвращаемого параметра <see cref="DataTable"/>
        /// </summary>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns><see cref="DataTable"/></returns>
        protected Task<DataTable> ExecFuncDataTable(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncDataTable(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете <paramref name="schemaName"/> с типом возвращаемого параметра <see cref="DataTable"/>
        /// </summary>
        /// <param name="schemaName">Имя схемы</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns><see cref="DataTable"/></returns>
        protected Task<DataTable> ExecFuncDataTable(string schemaName, string functionName, DbParameters parameters,
                                                    CancellationToken cancellationToken = default)
        {
            return ExecFuncDataTable(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию в пакете <paramref name="schemaName"/> с типом возвращаемого параметра <see cref="DataTable"/>
        /// </summary>
        /// <param name="schemaName">Имя схемы</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns><see cref="DataTable"/></returns>
        public static async Task<DataTable> ExecFuncDataTable(string schemaName, string functionName, DbParameters parameters,
                                                              NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            DataSet ds = await ExecFuncDataSet(schemaName, functionName, parameters, connection, cancellationToken).ConfigureAwait(false);
            if (ds != null)
            {
                DataTable table = ds.CopyToDataTable();
                if (table != null)
                {
                    table.TableName = schemaName + "." + functionName;
                }
                return table;
            }

            return null;
        }


        #region DataSet на основе REFCURSOR

        /// <summary>
        /// Выполняет функцию, возвращающую refcursor
        /// </summary>
        /// <param name="functionName">Имя функции в схеме</param>
        /// <param name="cancellationToken"></param>
        /// <returns><see cref="DataSet"/></returns>
        protected Task<DataSet> ExecFuncDataSetUsingCursor(string functionName, CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSetUsingCursor(SchemaName, functionName, null, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию, возвращающую refcursor
        /// </summary>
        /// <param name="functionName">Имя функции в схеме</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns><see cref="DataSet"/></returns>
        protected Task<DataSet> ExecFuncDataSetUsingCursor(string functionName, DbParameters parameters,
                                                           CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSetUsingCursor(SchemaName, functionName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию, возвращающую refcursor
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="functionName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <returns><see cref="DataSet"/></returns>
        protected Task<DataSet> ExecFuncDataSetUsingCursor(string schemaName, string functionName, DbParameters parameters,
                                                           CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSetUsingCursor(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        public static async Task<DataSet> ExecFuncDataSetUsingCursor(string schemaName, string functionName, DbParameters parameters,
                                                                     NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            IDataSetReader reader = await ExecuteFunctionCursor(schemaName, functionName, parameters, connection, cancellationToken)
                                            .ConfigureAwait(false);
            return new DataSet(reader);
        }

        private static async Task<IDataSetReader> ExecuteFunctionCursor(string schemaName, string functionName, DbParameters parameters,
                                                                        NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            object cursorName = await ExecuteFunction(schemaName, functionName, parameters, NpgsqlDbType.Refcursor, connection, cancellationToken)
                                        .ConfigureAwait(false);
            return new CursorReader(cursorName.ToString(), connection);
        }

        #endregion


        #region DataSet на основе TABLE или SETOF

        /// <summary>
        /// Выполняет функцию, возвращающую TABLE или SETOF
        /// </summary>
        /// <param name="functionName">Имя функции</param>
        /// <returns></returns>
        public Task<DataSet> ExecFuncDataSet(string functionName, CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSet(functionName, null, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию, возвращающую TABLE или SETOF
        /// </summary>
        /// <param name="functionName">Имя функции</param>
        /// <param name="parameters">Аргументы функции</param>
        /// <returns></returns>
        public Task<DataSet> ExecFuncDataSet(string functionName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSet(SchemaName, functionName, parameters, cancellationToken);
        }

        /// <summary>
        /// Выполняет функцию, возвращающую TABLE или SETOF
        /// </summary>
        /// <param name="schemaName">Имя схемы</param>
        /// <param name="functionName">Имя функции</param>
        /// <param name="parameters">Аргументы функции</param>
        /// <returns></returns>
        public Task<DataSet> ExecFuncDataSet(string schemaName, string functionName, DbParameters parameters,
                                             CancellationToken cancellationToken = default)
        {
            return ExecFuncDataSet(schemaName, functionName, parameters, Connection, cancellationToken);
        }

        public static async Task<DataSet> ExecFuncDataSet(string schemaName, string functionName, DbParameters parameters,
                                                          NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            NpgsqlDataReader npgsqlReader = await ExecuteFunctionTable(schemaName, functionName, parameters, connection, cancellationToken)
                                                    .ConfigureAwait(false);
            IDataSetReader dsReader = new TableReader(npgsqlReader);
            return new DataSet(dsReader);
        }

        private static async Task<NpgsqlDataReader> ExecuteFunctionTable(string schemaName, string functionName, DbParameters parameters,
                                                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            using NpgsqlCommand command = CreateCommandForStoredFunc(schemaName, functionName, parameters, connection);
            return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        }

        #endregion


        public static async Task<object> ExecuteFunction(string schemaName, string functionName, DbParameters parameters, NpgsqlDbType outputType,
                                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            using (var command = CreateCommandForStoredFunc(schemaName, functionName, parameters, connection))
            {
                var returnParam = new NpgsqlParameter { Direction = ParameterDirection.ReturnValue, NpgsqlDbType = outputType };
                if (outputType == NpgsqlDbType.Varchar)
                    returnParam.Size = DbParameters.DefaultVarcharSize;
                command.Parameters.Add(returnParam);

                return await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Создать команду для выполнения хранимой функции
        /// </summary>
        /// <param name="schemaName">Имя схемы</param>
        /// <param name="funcName">Имя функции в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        /// <returns></returns>
        private static NpgsqlCommand CreateCommandForStoredFunc(string schemaName, string funcName, DbParameters parameters,
                                                                NpgsqlConnection connection)
        {
            var command = new NpgsqlCommand((string.IsNullOrWhiteSpace(schemaName) ? "" : schemaName + ".") + funcName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };

            if (parameters != null && parameters.Count > 0)
            {
                foreach (NpgsqlParameter parameter in parameters.Values)
                    command.Parameters.Add(parameter);
            }

            return command;
        }

        /// <summary>
        /// Асинхронно выполняет хранимую процедуру в схеме <see cref="SchemaName"/>
        /// </summary>
        /// <param name="procedureName">Имя процедуры в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task CallProc(string procedureName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return CallProc(SchemaName, procedureName, parameters, cancellationToken);
        }

        /// <summary>
        /// Асинхронно выполняет хранимую процедуру
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="procedureName">Имя процедуры в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        protected Task CallProc(string schemaName, string procedureName, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return CallProc(schemaName, procedureName, parameters, Connection, cancellationToken);
        }

        /// <summary>
        /// Асинхронно выполняет хранимую процедуру
        /// </summary>
        /// <param name="schemaName">Имя схемы (если null или пустая строка, процедура вызывается без указания схемы)</param>
        /// <param name="procedureName">Имя процедуры в пакете</param>
        /// <param name="parameters">Входные параметры</param>
        /// <param name="connection">Подключение к базе</param>
        public static async Task CallProc(string schemaName, string procedureName, DbParameters parameters, NpgsqlConnection connection,
                                          CancellationToken cancellationToken = default)
        {
            string schemaSpecifier = string.IsNullOrWhiteSpace(schemaName) ? "" : $"{schemaName}.";

            string paramPlaceholders = string.Empty;
            if (parameters?.Count > 0)
                paramPlaceholders = string.Join(",", parameters.Select(p => $"@{p.Key}"));

            using (var cmd = new NpgsqlCommand($"CALL {schemaSpecifier}{procedureName}({paramPlaceholders})", connection))
            {
                if (parameters?.Count > 0)
                    foreach (NpgsqlParameter parameter in parameters.Values)
                        cmd.Parameters.Add(parameter);

                await cmd.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
