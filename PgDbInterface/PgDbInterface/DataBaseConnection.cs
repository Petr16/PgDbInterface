using Npgsql;

namespace PgDbInterface
{
    public class DataBaseConnection
    {
        /// <summary>
        /// Коннект к базе
        /// </summary>
        public NpgsqlConnection Connection { get; set; }
    }
}
