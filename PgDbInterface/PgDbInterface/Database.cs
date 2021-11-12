using System;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;

namespace PgDbInterface
{
    public class Database : IDisposable
    {
        private DataBaseConnection _dbConnection = new DataBaseConnection();
        private ConnectionInfo _connectionInfo = new ConnectionInfo();
        private NpgsqlTransaction _transaction;
        private QueryApi _query;


        /// <summary>
        /// API для работы с БД (методы для управления подключением и транзакциями)
        /// </summary>
        /// <param name="connection">Соединение с БД</param>
        public Database(DbConnection connection)
        {
            if (!(connection is NpgsqlConnection npgsqlConnection))
                throw new ArgumentException("Ошибка при создании Database: объект подключения не удалось привести к типу NpgsqlConnection.");
            Connection = npgsqlConnection;
        }

        /// <summary>
        /// API для работы с БД (методы для управления подключением и транзакциями)
        /// </summary>
        /// <param name="connectionString"></param>
        public Database(string connectionString)
        {
            Connection = new NpgsqlConnection(connectionString);
        }

        [Obsolete]
        public Database(ConnectionInfo connectionInfo, int timeOut = 30)
        {
            _connectionInfo = connectionInfo;
            Connection = new NpgsqlConnection(BuildConnectionString(_connectionInfo, timeOut));
        }

        private string BuildConnectionString(ConnectionInfo conInfo, int timeOut = 30)
        {
            var pgCSB = new NpgsqlConnectionStringBuilder
            {
                Pooling = false,
                Host = conInfo.Host,
                Port = conInfo.Port,
                Database = conInfo.DbName,
                Username = conInfo.User,
                Password = conInfo.Password,
                Timeout = timeOut
            };
            return pgCSB.ConnectionString;
        }


        public DataBaseConnection DBConnection
        {
            get { return _dbConnection; }
            set { _dbConnection = value; }
        }

        public NpgsqlConnection Connection
        {
            get { return _dbConnection.Connection; }
            set { _dbConnection.Connection = value; }
        }

        /// <summary>
        /// Соединение с БД открыто
        /// </summary>
        public bool Connected
        {
            get
            {
                return Connection != null &&
                       Connection.State != ConnectionState.Closed &&
                       Connection.State != ConnectionState.Broken;
            }
        }

        public ConnectionInfo ConnectionInfo
        {
            get { return _connectionInfo; }
        }

        /// <summary>
        /// API для выполнения произвольных SQL-команд
        /// </summary>
        public QueryApi Query => _query ??= new QueryApi(DBConnection);


        #region Connect & Disconnect

        public void Connect()
        {
            Connection.Open();
        }

        /// <summary>
        /// Открыть соединение с БД
        /// </summary>
        /// <param name="cancellationToken">Токен для отмены попытки соединения</param>
        /// <returns></returns>
        public Task ConnectAsync(CancellationToken cancellationToken = default(CancellationToken))
        {
            return Connection.OpenAsync(cancellationToken);
        }

        public void Connect(string connectString)
        {
            Connection = new NpgsqlConnection(connectString);
            Connection.Open();
        }

        public void Connect(ConnectionInfo conInfo, int timeOut = 30)
        {
            _connectionInfo = conInfo;
            Connection = new NpgsqlConnection(BuildConnectionString(_connectionInfo, timeOut));
            Connection.Open();
        }

        public void Connect(string userName, string password, string host, string dbName, int port)
        {
            var conInfo = new ConnectionInfo { User = userName, Password = password, Host = host, DbName = dbName, Port = port };
            Connect(conInfo);
        }

        public void Disconnect()
        {
            if (Connection != null)
                Connection.Close();
        }

        /// <summary>
        /// Закрыть соединение с БД
        /// </summary>
        /// <returns></returns>
        public Task DisconnectAsync()
        {
            if (Connection == null)
                return Task.CompletedTask;

            return Connection.CloseAsync();
        }

        #endregion


        /// <summary>
        /// Начать транзакцию, управляемую через возвращаемый объект типа <see cref="NpgsqlTransaction"/>
        /// </summary>
        /// <returns></returns>
        public NpgsqlTransaction StartTransaction()
        {
            return Connection.BeginTransaction();
        }

        /// <summary>
        /// Начать транзакцию, управляемую методами <see cref="Commit"/> и <see cref="Rollback"/>
        /// </summary>
        public void BeginTransaction()
        {
            _transaction = Connection.BeginTransaction();
        }

        public void Commit()
        {
            _transaction.Commit();
        }

        public void Rollback()
        {
            _transaction.Rollback();
        }

        public void Dispose()
        {
            Connection.Dispose();
            Connection = null;
            _connectionInfo = null;
            GC.SuppressFinalize(this);
        }

        public async Task DisposeAsync()
        {
            await Connection.DisposeAsync().ConfigureAwait(false);
            Connection = null;
            _connectionInfo = null;
            GC.SuppressFinalize(this);
        }

        public override string ToString()
        {
            return _connectionInfo.ToString();
        }
    }
}
