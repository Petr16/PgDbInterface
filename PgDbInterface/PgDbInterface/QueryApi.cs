using Npgsql;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface
{
    /// <summary>
    /// API для выполнения произвольных SQL-команд
    /// </summary>
    public class QueryApi
    {
        private readonly NpgsqlConnection _connection;

        /// <summary>
        /// API для выполнения произвольных SQL-команд
        /// </summary>
        /// <param name="connection">Соединение с БД</param>
        public QueryApi(DataBaseConnection connection)
        {
            _connection = connection.Connection;
        }

        /// <summary>
        /// Выполнить SQL-команду и вернуть <see cref="DataSet"/>
        /// </summary>
        /// <param name="queryText">Текст SQL-команды</param>
        /// <param name="parameters">Параметры для команды</param>
        /// <returns></returns>
        public Task<DataSet> ExecuteDataSet(string queryText, DbParameters parameters, CancellationToken cancellationToken = default)
        {
            return ExecuteDataSet(queryText, parameters, _connection, cancellationToken);
        }

        /// <summary>
        /// Выполнить SQL-команду и вернуть <see cref="DataSet"/>
        /// </summary>
        /// <param name="queryText">Текст SQL-команды</param>
        /// <param name="parameters">Параметры для команды</param>
        /// <param name="connection">Соединение с БД</param>
        /// <returns></returns>
        public static async Task<DataSet> ExecuteDataSet(string queryText, DbParameters parameters,
                                                         NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            NpgsqlDataReader npgsqlReader = await ExecuteReader(queryText, parameters, connection, cancellationToken).ConfigureAwait(false);
            IDataSetReader dsReader = new TableReader(npgsqlReader);
            return new DataSet(dsReader);
        }

        private static async Task<NpgsqlDataReader> ExecuteReader(string queryText, DbParameters parameters,
                                                                  NpgsqlConnection connection, CancellationToken cancellationToken = default)
        {
            using NpgsqlCommand command = CreateCommand(queryText, parameters, connection);
            return await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        }

        private static NpgsqlCommand CreateCommand(string queryText, DbParameters parameters, NpgsqlConnection connection)
        {
            var command = new NpgsqlCommand(queryText, connection);
            if (parameters?.Count > 0)
            {
                foreach (NpgsqlParameter parameter in parameters.Values)
                    command.Parameters.Add(parameter);
            }

            return command;
        }
    }
}
