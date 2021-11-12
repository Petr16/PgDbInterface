using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace PgDbInterface
{
    public class DataSet : IDisposable
    {
        private Dictionary<string, int> _fieldIndexDictionary = new Dictionary<string, int>();
        private IDataSetReader _reader;
        private IDataRecord _record;

        public DataSet(IDataSetReader rd)
        {
            _reader = rd;
        }

        public int RecordsAffected => _reader.RecordsAffected;

        /// <summary>
        /// количество полей в возвращаемом наборе данных
        /// </summary>
        public int FieldCount
        {
            get { return _reader.FieldCount; }
        }

        /// <summary>
        /// Переход к следующей записи
        /// </summary>
        /// <returns>true - если есть следующая запись и переход прошел успешно</returns>
        public bool Read()
        {
            bool readSuccessful = _reader.Read();
            _record = _reader.DataRecord;
            return readSuccessful;
        }

        /// <summary>
        /// Переход к следующей записи
        /// </summary>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<bool> ReadAsync(CancellationToken cancellationToken = default)
        {
            bool readSuccessful = await _reader.ReadAsync(cancellationToken).ConfigureAwait(false);
            _record = _reader.DataRecord;
            return readSuccessful;
        }

        /// <summary>
        /// Закрыть ридер
        /// </summary>
        public void Close()
        {
            _reader.Close();
        }

        /// <summary>
        /// Получить значение поля с указанным типом данных
        /// </summary>
        /// <typeparam name="T">Тип данных для значения поля</typeparam>
        /// <param name="fieldName">Имя поля</param>
        /// <returns></returns>
        public T GetFieldValue<T>(string fieldName)
        {
            return ((DbDataReader)_record).GetFieldValue<T>(fieldName);
        }

        /// <summary>
        /// Получить значение по индексу поля <paramref name="index"/>
        /// </summary>
        /// <param name="index">индекс поля</param>
        /// <returns></returns>
        public object GetValue(int index)
        {
            return _record.GetValue(index);
        }

        /// <summary>
        /// Получить значение по имени поля <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        /// <returns></returns>
        public object GetValue(string fieldName)
        {
            return _record.GetValue(GetIdxByFieldName(fieldName));
        }

        /// <summary>
        /// Получить индекс поля по его имени <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        /// <returns></returns>
        private int GetIdxByFieldName(string fieldName)
        {
            int index;
            if (_fieldIndexDictionary.TryGetValue(fieldName, out index))
                return index;
            try
            {
                index = _record.GetOrdinal(fieldName);
                _fieldIndexDictionary.Add(fieldName, index);
            }
            catch (Exception ex)
            {
                throw new Exception($"Поле [{fieldName}] не найдено в DataSet, полученном с сервера", ex);
            }

            return index;
        }

        /// <summary>
        /// Получить имя поля по индексу <paramref name="index"/>
        /// </summary>
        /// <param name="index">индекс поля</param>
        /// <returns></returns>
        public string GetFieldName(int index)
        {
            return _record.GetName(index);
        }

        /// <summary>
        /// Возвращает тип данных в колонке <paramref name="index"/>
        /// </summary>
        /// <param name="index">Индекс колонки для которой узнаем Тип данных</param>
        /// <returns>Тип данных в колонке</returns>
        public Type GetFieldType(int index)
        {
            return _record.GetFieldType(index);
        }

        /// <summary>
        /// Проверка поля <paramref name="fieldName"/> на null
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        /// <returns>null, если значение типа DBNull</returns>
        public bool IsNull(string fieldName)
        {
            return _record.IsDBNull(GetIdxByFieldName(fieldName));
        }

        /// <summary>
        /// Проверка поля с индексом <paramref name="index"/> на null
        /// </summary>
        /// <param name="index">индекс поля</param>
        /// <returns>null, если значение типа DBNull</returns>
        public bool IsNull(int index)
        {
            return _record.IsDBNull(index);
        }

        /// <summary>
        /// Признак закрытого курсора
        /// </summary>
        /// <returns></returns>
        public bool IsClosed()
        {
            return _reader.IsClosed;
        }

        /// <summary>
        /// Проверка, есть ли поле <paramref name="name"/> в наборе данных
        /// </summary>
        /// <param name="name">имя поля</param>
        /// <returns></returns>
        public bool IsFieldExists(String name)
        {
            try
            {
                _record.GetOrdinal(name);
                return true;
            }
            catch
            {
                return false;
            }
        }

        /// <summary>
        /// Копирует Датасет как строки в таблицу, создает колонки с именами полей автоматически
        /// </summary>
        /// <param name="dt">DataTable - приемник</param>
        /// <param name="skipColumns">true - если не нужно создавать колонки</param>
        public void CopyToDataTable(DataTable dt, bool skipColumns)
        {
            if (!skipColumns)
            {
                for (int i = 0; i < FieldCount; i++)
                    dt.Columns.Add(GetFieldName(i), GetFieldType(i));
            }

            while (Read())
            {
                DataRow row = dt.NewRow();
                for (int i = 0; i < FieldCount; i++)
                    row[i] = GetValue(i);
                dt.Rows.Add(row);
            }
            Close();
        }

        /// <summary>
        /// Копирует Датасет как строки в таблицу, создает колонки с именами полей автоматически
        /// </summary>
        /// <returns></returns>
        public DataTable CopyToDataTable()
        {
            var dt = new DataTable();
            CopyToDataTable(dt, false);
            return dt;
        }

        #region Методы возвращающие значения

        /// <summary>
        /// Возвращает значение типа <see cref="string"/> для поля с индексом <paramref name="index"/>
        /// </summary>
        /// <param name="index">индекс поля</param>
        /// <returns>string</returns>
        public String GetString(int index)
        {
            if (IsNull(index))
                return string.Empty;
            return _record.GetValue(index).ToString();
        }

        /// <summary>
        /// Возвращает значение типа <see cref="decimal"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public decimal? GetDecimal(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToDecimal(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="int"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public int? GetInt(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToInt32(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="int"/> для поля с именем <paramref name="fieldName"/>.
        /// Если поля нет в DataSet или его значение равно <c>null</c>, возвращает 0
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public int GetIntNotNull(string fieldName)
        {
            if (IsNull(fieldName))
                return default;

            int? fieldValue = GetInt(fieldName);
            return fieldValue ?? default;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="short"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public short? GetInt16(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToInt16(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="short"/> для поля с именем <paramref name="fieldName"/>.
        /// Если поля нет в DataSet или его значение равно <c>null</c>, возвращает 0
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public short GetInt16NotNull(string fieldName)
        {
            if (IsNull(fieldName))
                return default;

            short? fieldValue = GetInt16(fieldName);
            return fieldValue ?? default;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="long"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public long? GetInt64(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToInt64(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="long"/> для поля с именем <paramref name="fieldName"/>.
        /// Если поля нет в DataSet или его значение равно <c>null</c>, возвращает 0
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public long GetInt64NotNull(string fieldName)
        {
            if (IsNull(fieldName))
                return default;

            long? fieldValue = GetInt64(fieldName);
            return fieldValue ?? default;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="double"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public double? GetDouble(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToDouble(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="double"/> для поля с именем <paramref name="fieldName"/>.
        /// Если поля нет в DataSet или его значение равно <c>null</c>, возвращает 0
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public double GetDoubleNotNull(string fieldName)
        {
            if (IsNull(fieldName))
                return default;

            double? fieldValue = GetDouble(fieldName);
            return fieldValue ?? default;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="string"/> для поля с именем <paramref name="fieldName"/> 
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        /// <returns></returns>
        public string GetString(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return string.Empty;
            }
            if (obj.GetType() == typeof(byte[]))
                return BitConverter.ToString((byte[])obj).Replace("-", string.Empty);

            return obj.ToString();
        }

        /// <summary>
        /// Возвращает значение типа <see cref="DateTime"/> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        /// <returns></returns>
        public DateTime? GetDate(string fieldName)
        {
            if (IsNull(fieldName))
                return null;

            object obj = GetValue(fieldName);
            if ((obj == null) || (obj == DBNull.Value))
                return null;

            return (DateTime)obj;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="bool"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public bool? GetBool(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return Convert.ToBoolean(obj);
        }

        /// <summary>
        /// Возвращает значение типа <see cref="bool"/> для поля с именем <paramref name="fieldName"/>.
        /// Если поля нет в DataSet или его значение равно <c>null</c>, возвращает <c>false</c>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public bool GetBoolNotNull(string fieldName)
        {
            if (IsNull(fieldName))
                return false;

            bool? fieldValue = GetBool(fieldName);
            return fieldValue == true;
        }

        /// <summary>
        /// Возвращает значение типа <see cref="List"/> или <b>null</b> для поля с именем <paramref name="fieldName"/>
        /// </summary>
        /// <param name="fieldName">имя поля</param>
        public List<int> GetListNumber(string fieldName)
        {
            object obj = GetValue(fieldName);
            if (obj == null || obj is DBNull)
            {
                return null;
            }
            return (obj as int[]).ToList();
        }

        #endregion

        /// <summary>
        /// делаем Dispose для объекта - нужно для того чтобы завернуть в блок using
        /// </summary>
        public void Dispose()
        {
            if (!IsClosed())
                Close(); //при dispose нужно закрыть - чтобы курсор не оставался на сервере
            _reader = null;
            _record = null;
            _fieldIndexDictionary = null;
        }

    }
}
