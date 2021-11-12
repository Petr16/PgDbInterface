using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface
{
    /// <summary>
    /// Читает строки из набора данных, полученного из БД
    /// </summary>
    public interface IDataSetReader
    {
        /// <summary>
        /// Предоставляет доступ к значениям столбцов в строке данных
        /// </summary>
        IDataRecord DataRecord { get; }

        /// <summary>
        /// Количество столбцов в текущей строке
        /// </summary>
        int FieldCount { get; }

        /// <summary>
        /// Ридер закрыт
        ///</summary>
        bool IsClosed { get; }

        /// <summary>
        /// Количество строк, которые были изменены, вставлены или удалены в результате выполнения SQL-запроса
        /// </summary>
        int RecordsAffected { get; }

        /// <summary>
        /// Перейти к следующей строке в наборе данных
        /// </summary>
        /// <returns></returns>
        bool Read();

        /// <summary>
        /// Асинхронно перейти к следующей строке в наборе данных
        /// </summary>
        /// <returns></returns>
        Task<bool> ReadAsync(CancellationToken cancellationToken = default);

        /// <summary>
        /// Закрыть ридер
        /// </summary>
        void Close();
    }
}
