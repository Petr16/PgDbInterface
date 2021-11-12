using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface
{
    /// <summary>
    /// Читает строки из TABLE или SETOF
    /// </summary>
    internal class TableReader : IDataSetReader
    {
        private DbDataReader _reader;

        /// <summary>
        /// Читает строки из TABLE или SETOF
        /// </summary>
        /// <param name="reader">Ридер, наследующий DbDataReader</param>
        public TableReader(DbDataReader reader)
        {
            _reader = reader;
        }


        public IDataRecord DataRecord => _reader;

        public int FieldCount => _reader?.FieldCount ?? 0;

        public bool IsClosed => _reader?.IsClosed ?? false;

        public int RecordsAffected => _reader?.RecordsAffected ?? 0;


        public bool Read()
        {
            return _reader.Read();
        }

        public async Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            return await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
        }

        public void Close()
        {
            _reader.Close();
        }
    }
}
