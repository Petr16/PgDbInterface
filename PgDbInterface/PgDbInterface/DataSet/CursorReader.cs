using Npgsql;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface
{
    /// <summary>
    /// Читает строки из курсора
    /// </summary>
    internal class CursorReader : IDataSetReader
    {
        private readonly NpgsqlConnection _connection;
        private readonly string _cursorName;
        private readonly int _fetchSize;
        private NpgsqlDataReader _reader;

        /// <summary>
        /// Количество строк, прочитанных после предыдущего FETCH
        /// </summary>
        private int _readCount = -1;

        /// <summary>
        /// Читает строки из курсора
        /// </summary>
        /// <param name="cursorName">Имя курсора</param>
        /// <param name="connection">Соединение с БД</param>
        /// <param name="fetchSize">Количество строк, получаемых при каждом FETCH</param>
        public CursorReader(string cursorName, NpgsqlConnection connection, int fetchSize = 100)
        {
            _cursorName = cursorName;
            _connection = connection;
            _fetchSize = fetchSize;
        }


        public IDataRecord DataRecord => _reader;

        public int FieldCount => _reader?.FieldCount ?? 0;

        public bool IsClosed => _reader?.IsClosed ?? false;

        public int RecordsAffected => _reader?.RecordsAffected ?? 0;

        /// <summary>
        /// FETCH еще ни разу не выолнялся
        /// </summary>
        private bool FirstFetchNeeded => _readCount < 0;

        /// <summary>
        /// Прочитаны все строки, полученные в результате предыдущего FETCH
        /// </summary>
        private bool AllFetchedRowsRead => _readCount >= _fetchSize;


        public bool Read()
        {
            if (FirstFetchNeeded || AllFetchedRowsRead)
                Fetch();

            bool readSuccessful = _reader.Read();
            if (readSuccessful)
                _readCount++;

            return readSuccessful;
        }

        private void Fetch()
        {
            CloseReaderOfPrevFetch();
            _readCount = 0;

            using var fetchCommand = CreateFetchCommand();
            _reader = fetchCommand.ExecuteReader();
        }

        public async Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            if (FirstFetchNeeded || AllFetchedRowsRead)
                await FetchAsync(cancellationToken).ConfigureAwait(false);

            bool readSuccessful = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            if (readSuccessful)
                _readCount++;

            return readSuccessful;
        }

        private async Task FetchAsync(CancellationToken cancellationToken = default)
        {
            CloseReaderOfPrevFetch();
            _readCount = 0;

            using var fetchCommand = CreateFetchCommand();
            _reader = await fetchCommand.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Закрыть ридер, использовавшийся для строк предыдущего FETCH
        /// </summary>
        private void CloseReaderOfPrevFetch()
        {
            if (_reader != null)
                Close();
        }

        private NpgsqlCommand CreateFetchCommand()
        {
            return new NpgsqlCommand($"FETCH FORWARD {_fetchSize} IN \"{_cursorName}\"", _connection);
        }

        public void Close()
        {
            _reader.Close();
        }
    }
}
