using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using Npgsql;
using NpgsqlTypes;

namespace PgDbInterface
{
    /// <summary>
    /// Параметры SQL-команды
    /// </summary>
    public class DbParameters : IDictionary<string, NpgsqlParameter>
    {
        private readonly Dictionary<string, NpgsqlParameter> _parameters = new Dictionary<string, NpgsqlParameter>();

        public static int DefaultVarcharSize => 30000;

        /// <summary>
        /// Параметры SQL-команды
        /// </summary>
        public DbParameters() { }

        /// <summary>
        /// Параметры SQL-команды
        /// </summary>
        /// <param name="paramValues">Значения параметров</param>
        /// <param name="unknownValueType">Использовать тип данных <see cref="NpgsqlDbType.Unknown"/>
        ///                                (только для параметров, переданных в данный конструктор)</param>
        public DbParameters(IDictionary<string, object> paramValues, bool unknownValueType = false)
        {
            if (paramValues == null)
                return;

            foreach (var pv in paramValues)
            {
                if (unknownValueType)
                    Add(pv.Key, pv.Value, NpgsqlDbType.Unknown);
                else
                    Add(pv.Key, pv.Value);
            }
        }

        #region Методы

        /// <summary>
        /// Добавляет в коллекцию параметров значение параметра
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter Add(string parameterName, object value)
        {
            return Add(parameterName, value, -1);
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение параметра с <see cref="ParameterDirection.Input"/>
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <param name="size">Размер параметра</param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter Add(string parameterName, object value, int size)
        {
            return Add(parameterName, value, size, ParameterDirection.Input);
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение параметра
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <param name="size">Размер параметра</param>
        /// <param name="direction"></param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter Add(string parameterName, object value, int size, ParameterDirection direction)
        {
            NpgsqlDbType dataType = GetDbType(value);
            return Add(parameterName, value, size, direction, dataType);
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение параметра.
        /// <para/>
        /// Данную перегрузку рекомендуется использовать, когда невозможно определить тип параметра для БД по CLR-типу
        /// (например, в случае c JSON, который представляется в виде string).
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <param name="dataType">Тип данных для значения параметра</param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter Add(string parameterName, object value, NpgsqlDbType dataType)
        {
            return Add(parameterName, value, -1, ParameterDirection.Input, dataType);
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение параметра
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <param name="size">Размер параметра</param>
        /// <param name="direction"></param>
        /// <param name="dataType"></param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter Add(string parameterName, object value, int size, ParameterDirection direction, NpgsqlDbType dataType)
        {
            if (value == null || (value is DateTime d && d == DateTime.MinValue))
                value = DBNull.Value;

            var param = new NpgsqlParameter(parameterName, dataType) { Value = value, Direction = direction };
            if (size >= 0)
            {
                param.Size = size;
            }
            else if (dataType == NpgsqlDbType.Varchar)
            {
                if (direction == ParameterDirection.Output || direction == ParameterDirection.InputOutput)
                    param.Size = DefaultVarcharSize;
                else if (value is string)
                    param.Size = ((string)value).Length;
            }

            Add(parameterName, param);
            return param;
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение возвращаемого параметра
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter AddOutParam(string parameterName, object value)
        {
            return AddOutParam(parameterName, value, -1, GetDbType(value));
        }

        /// <summary>
        /// Добавляет в коллекцию параметров значение возвращаемого параметра
        /// </summary>
        /// <param name="parameterName">Имя параметра</param>
        /// <param name="value">Значение параметра</param>
        /// <param name="size">Размер возвращаемого значения</param>
        /// <param name="dataType"></param>
        /// <returns>Добавленный параметр</returns>
        public NpgsqlParameter AddOutParam(string parameterName, object value, int size, NpgsqlDbType dataType)
        {
            return Add(parameterName, value, size, ParameterDirection.InputOutput, dataType);
        }

        private NpgsqlParameter GetOutParameter(string key)
        {
            NpgsqlParameter param = this[key];
            if (param.Direction == ParameterDirection.InputOutput || param.Direction == ParameterDirection.Output)
            {
                return this[key];
            }
            return null;
        }

        #region Обновление Out параметров

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref string value)
        {
            NpgsqlParameter parameter = GetOutParameter(key);
            if (parameter != null)
            {
                value = parameter.Value is string ? (string)parameter.Value : null;
                return;
            }
            throw new KeyNotFoundException(string.Format("Параметр \"{0}\" не является Out параметром", key));
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref decimal value)
        {
            decimal? val = value;
            UpdateOutParam(key, ref val);
            value = val ?? 0;
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref decimal? value)
        {
            NpgsqlParameter parameter = GetOutParameter(key);
            if (parameter != null)
            {
                value = parameter.Value is decimal ? (decimal?)parameter.Value : null;
                return;
            }
            throw new KeyNotFoundException(string.Format("Параметр \"{0}\" не является Out параметром", key));
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref int value)
        {
            int? val = value;
            UpdateOutParam(key, ref val);
            value = val ?? 0;
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref int? value)
        {
            NpgsqlParameter parameter = GetOutParameter(key);
            if (parameter != null)
            {
                value = parameter.Value == null ? null : (int?)Convert.ToInt32(parameter.Value);
                return;
            }
            throw new KeyNotFoundException(string.Format("Параметр \"{0}\" не является Out параметром", key));
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref double? value)
        {
            NpgsqlParameter parameter = GetOutParameter(key);
            if (parameter != null)
            {
                value = parameter.Value is double ? (double?)parameter.Value : null;
                return;
            }
            throw new KeyNotFoundException(string.Format("Параметр \"{0}\" не является Out параметром", key));
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref double value)
        {
            double? val = value;
            UpdateOutParam(key, ref val);
            value = val ?? 0;
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref DateTime? value)
        {
            NpgsqlParameter parameter = GetOutParameter(key);
            if (parameter != null)
            {
                if (parameter.NpgsqlDbType == NpgsqlDbType.Date || parameter.NpgsqlDbType == NpgsqlDbType.Time || parameter.NpgsqlDbType == NpgsqlDbType.Timestamp)
                {
                    value = (DateTime?)parameter.Value;
                }
                else
                {
                    value = null;
                }
                return;
            }
            throw new KeyNotFoundException(string.Format("Параметр \"{0}\" не является Out параметром", key));
        }

        /// <summary>
        /// Обновляет значение <paramref name="value"/> значением из параметра c именем <paramref name="key"/>.
        /// </summary>
        /// <param name="key">Имя параметра</param>
        /// <param name="value">Значение которое обновляем</param>
        public void UpdateOutParam(string key, ref DateTime value)
        {
            DateTime? val = value;
            UpdateOutParam(key, ref val);
            value = val ?? DateTime.MinValue;
        }

        #endregion

        #endregion

        #region Статические методы

        /// <summary>
        /// Возвращает тип <see cref="NpgsqlDbType"/> для переданного значения
        /// </summary>
        /// <param name="value">Объект для которого хотим узнать тип данных из перечисления <see cref="NpgsqlDbType"/></param>
        /// <returns>Значение типа для переданного объекта</returns>
        /// <exception cref="ArgumentException">Если тип параметра <paramref name="value"/> не поддерживается данным методом,
        ///  т.е. для него не определен тип из перечисления <see cref="NpgsqlDbType"/></exception>
        public static NpgsqlDbType GetDbType(object value)
        {
            if (value == null || value is DBNull)
                return NpgsqlDbType.Unknown;

            return GetDbTypeForType(value.GetType());
        }

        /// <summary>
        /// Возвращает тип <see cref="NpgsqlDbType"/> соответствующий типу <paramref name="type"/>
        /// </summary>
        /// <param name="type">Тип для которого хотим узнать тип данных из перечисления <see cref="NpgsqlDbType"/></param>
        /// <returns>Значение типа <see cref="NpgsqlDbType"/> для переданного типа <paramref name="type"/></returns>
        /// <exception cref="ArgumentException">Если тип <paramref name="type"/> не поддерживается данным методом,
        ///  т.е. для него не определен тип из перечисления <see cref="NpgsqlDbType"/></exception>
        public static NpgsqlDbType GetDbTypeForType(Type type)
        {
            if (type == null)
                return NpgsqlDbType.Unknown;

            if (type == typeof(byte[]))
                return NpgsqlDbType.Bytea;

            if (type == typeof(string))
                return NpgsqlDbType.Text;

            if (type == typeof(decimal))
                return NpgsqlDbType.Numeric;

            if (type == typeof(float))
                return NpgsqlDbType.Real;

            if (type == typeof(int))
                return NpgsqlDbType.Integer;

            if (type == typeof(short))
                return NpgsqlDbType.Smallint;

            if (type == typeof(long))
                return NpgsqlDbType.Bigint;

            if (type == typeof(DateTime))
                return NpgsqlDbType.Timestamp;

            if (type == typeof(double))
                return NpgsqlDbType.Double;

            if (type == typeof(bool))
                return NpgsqlDbType.Boolean;

            if (type == typeof(char))
                return NpgsqlDbType.Char;

            if (type.IsArray)
                return GetDbTypeForArray(type);

            if (typeof(IEnumerable).IsAssignableFrom(type) && type.IsGenericType)
                return GetDbTypeForGenericIEnumerable(type);

            throw new ArgumentException(string.Format("Тип {0} не поддерживается", type));
        }

        private static NpgsqlDbType GetDbTypeForArray(Type arrayType)
        {
            try
            {
                return NpgsqlDbType.Array | GetDbTypeForType(arrayType.GetElementType());
            }
            catch (Exception)
            {
                return NpgsqlDbType.Unknown;
            }
        }

        private static NpgsqlDbType GetDbTypeForGenericIEnumerable(Type enumerableType)
        {
            try
            {
                Type itemType = enumerableType.GetGenericArguments()[0];
                return NpgsqlDbType.Array | GetDbTypeForType(itemType);
            }
            catch (Exception)
            {
                return NpgsqlDbType.Unknown;
            }
        }

        /// <summary>
        /// Конвертирует результат выполнения функции в тип <see cref="decimal"/>
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        public static decimal? ResultToDecimal(object result)
        {
            if (result is NpgsqlParameter)
                result = ((NpgsqlParameter)result).Value;

            return result is decimal ? (decimal?)result : null;
        }

        /// <summary>
        /// Конвертирует результат выполнения функции в тип <see cref="double"/>
        /// </summary>
        /// <param name="result"></param>
        /// <returns></returns>
        public static double? ResultToDouble(object result)
        {
            if (result is NpgsqlParameter)
                result = ((NpgsqlParameter)result).Value;

            return result is double ? (double?)result : null;
        }

        #endregion

        #region Реализация IDictionary

        #region Implementation of IEnumerable

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        #region Implementation of ICollection<KeyValuePair<string,OracleParameter>>

        public void Add(KeyValuePair<string, NpgsqlParameter> item)
        {
            _parameters.Add(item.Key, item.Value);
        }

        public void Clear()
        {
            _parameters.Clear();
        }

        public bool Contains(KeyValuePair<string, NpgsqlParameter> item)
        {
            return ((ICollection<KeyValuePair<string, NpgsqlParameter>>)_parameters).Contains(item);
        }

        public void CopyTo(KeyValuePair<string, NpgsqlParameter>[] array, int arrayIndex)
        {
            ((ICollection<KeyValuePair<string, NpgsqlParameter>>)_parameters).CopyTo(array, arrayIndex);
        }

        public bool Remove(KeyValuePair<string, NpgsqlParameter> item)
        {
            return ((ICollection<KeyValuePair<string, NpgsqlParameter>>)_parameters).Remove(item);
        }

        public int Count
        {
            get { return _parameters.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        #endregion

        #region Implementation of IEnumerable<KeyValuePair<string,OracleParameter>>

        public IEnumerator<KeyValuePair<string, NpgsqlParameter>> GetEnumerator()
        {
            return ((IEnumerable<KeyValuePair<string, NpgsqlParameter>>)_parameters).GetEnumerator();
        }

        #endregion

        #region Implementation of IDictionary<string,OracleParameter>

        public bool ContainsKey(string key)
        {
            return _parameters.ContainsKey(key);
        }

        public void Add(string key, NpgsqlParameter value)
        {
            Add(new KeyValuePair<string, NpgsqlParameter>(key, value));
        }

        public bool Remove(string key)
        {
            return _parameters.Remove(key);
        }

        public bool TryGetValue(string key, out NpgsqlParameter value)
        {
            return _parameters.TryGetValue(key, out value);
        }

        public NpgsqlParameter this[string key]
        {
            get
            {
                if (ContainsKey(key))
                {
                    return _parameters[key];
                }
                throw new KeyNotFoundException(string.Format("Коллекция не содержит параметра с именем\"{0}\"", key));
            }
            set { _parameters[key] = value; }
        }

        public ICollection<string> Keys
        {
            get { return _parameters.Keys; }
        }

        public ICollection<NpgsqlParameter> Values
        {
            get { return _parameters.Values; }
        }

        #endregion

        #endregion
    }
}
