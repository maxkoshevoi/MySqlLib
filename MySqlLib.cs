﻿using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;

namespace MySqlLib
{
    /// <summary>Класс для простой работы с MySQL сервером.</summary>
    public class MySqlData : ICloneable, IDisposable
    {
        #region variables
        public bool ClearParametersAfterQuery { get; set; } = false;

        private MySqlConnection connection;
        private MySqlCommand _command;
        private MySqlCommand command
        {
            get { return _command; }
            set
            {
                _command = value;
                Parameters = new MySqlDataParameters(command);
            }
        }
        public MySqlDataErrorPattern ErrorPattern { get; set; }
        public MySqlDataParameters Parameters { get; private set; }

        /// <summary>Задаёт или возвращает таймаут для выполнения команды.</summary>
        public int CommandTimeout
        {
            get { return command.CommandTimeout; }
            set { command.CommandTimeout = value; }
        }

        /// <summary>Возвращаемые данные.</summary>
        public class MySqlDataResult<T> : MySqlDataVoid
        {
            /// <summary>
            /// Возвращает результат запроса.
            /// </summary>
            public T Result;

            public MySqlDataResult() { }

            public MySqlDataResult(T result, MySqlDataError error, bool isError) : base(error, isError)
            {
                Result = result;
            }

            public void Dispose() { }
        }

        /// <summary>Возвращаемые данные.</summary>
        public class MySqlDataVoid
        {
            /// <summary>
            /// Возвращает текст ошибки.
            /// </summary>
            public MySqlDataError Error;
            /// <summary>
            /// Возвращает True, если произошла ошибка.
            /// </summary>
            public bool HasError;

            public MySqlDataVoid() { }

            public MySqlDataVoid(MySqlDataError error, bool isError)
            {
                Error = error;
                HasError = isError;
            }
        }

        public class MySqlDataError
        {
            public string Message { get; private set; }
            public int Number { get; private set; }
            private Exception source;
            private MySqlDataErrorPattern ePattern;

            public Exception Source
            {
                get
                {
                    return source;
                }
                set
                {
                    source = value;

                    if (Source is MySqlException)
                    {
                        var tmp = ParceError((MySqlException)source, ePattern);
                        Number = tmp.Number;
                        Message = tmp.Message;
                    }
                    else
                    {
                        Message = Source.Message;
                        Number = -1;
                    }
                }
            }

            public static MySqlDataError ParceError(MySqlException ex, MySqlDataErrorPattern ePattern)
            {
                MySqlDataError mr = new MySqlDataError()
                {
                    source = ex,
                    ePattern = ePattern
                };
                if (ex.Number == 0 && ex.InnerException is MySqlException)
                {
                    ex = ex.InnerException as MySqlException;
                }
                mr.Number = ex.Number;
                if (ePattern.ContainsPattern(ex.Number))
                {
                    mr.Message = ePattern.GetPatternByCode(ex.Number);
                }
                else
                {
                    mr.Message = ex.Message;
                }
                mr.Message = ePattern.Prefix + mr.Message + ePattern.Postfix;
                return mr;
            }

            private MySqlDataError() { }

            public MySqlDataError(MySqlException ex, MySqlDataErrorPattern errorPattern)
            {
                ePattern = errorPattern;
                Source = ex;
            }

            public MySqlDataError(Exception ex)
            {
                Source = ex;
            }

            public override string ToString()
            {
                return Message;
            }

        }

        /// <summary>Содержит сообщения об ошибках, которые заменяют стандартные</summary>
        public class MySqlDataErrorPattern
        {
            private const int host = 1042, login = 1045, db = 1049;
            public string UnableToConnectToHost
            {
                get { return GetPatternByCode(host); }
                set { AddPatternByCode(host, value); }
            }
            public string InvalidLoginOrPassword
            {
                get { return GetPatternByCode(login); }
                set { AddPatternByCode(login, value); }
            }
            public string UnknownDatabase
            {
                get { return GetPatternByCode(db); }
                set { AddPatternByCode(db, value); }
            }

            public string Prefix { get; set; } = "";
            public string Postfix { get; set; } = "";
            private Dictionary<int, string> ByCode = new Dictionary<int, string>();

            public void AddPatternByCode(int code, string errorString)
            {
                if (ByCode.ContainsKey(code))
                {
                    ByCode[code] = errorString;
                }
                else
                {
                    ByCode.Add(code, errorString);
                }
            }

            public bool ContainsPattern(int code)
            {
                return ByCode.ContainsKey(code);
            }

            public string GetPatternByCode(int code)
            {
                if (ByCode.ContainsKey(code))
                {
                    return ByCode[code];
                }
                else
                {
                    throw new ArgumentOutOfRangeException();
                }
            }

            public void RemovePatternByCode(int code)
            {
                if (ByCode.ContainsKey(code))
                {
                    ByCode.Remove(code);
                }
            }
        }

        public class MySqlDataParameters
        {
            private MySqlCommand command;

            internal MySqlDataParameters(MySqlCommand command)
            {
                this.command = command;
            }

            public void AddRange(params MySqlParameter[] parameters)
            {
                command.Parameters.AddRange(parameters);
            }

            public void AddWithValue(string name, object value)
            {
                command.Parameters.AddWithValue(name, value);
            }

            public void Clear()
            {
                command.Parameters.Clear();
            }
        }
        #endregion

        public MySqlData()
        {
            connection = new MySqlConnection();
            ErrorPattern = new MySqlDataErrorPattern();
            command = new MySqlCommand();
        }

        public object Clone()
        {
            MySqlData newMD = new MySqlData()
            {
                connection = (MySqlConnection)connection?.Clone(),
                command = (MySqlCommand)command?.Clone(),
                ErrorPattern = ErrorPattern,
                ClearParametersAfterQuery = ClearParametersAfterQuery
            };
            if (newMD.command != null)
            {
                newMD.command.Connection = newMD.connection;
            }
            return newMD;
        }

        public void Dispose()
        {
            command.Dispose();
            connection.Dispose();
        }

        public MySqlDataVoid TestConnection()
        {
            MySqlDataVoid mr = OpenConnection();
            if (!mr.HasError)
            {
                connection.Close();
            }
            return mr;
        }

        public void RecreateCommand()
        {
            command = new MySqlCommand()
            {
                Connection = connection
            };
        }

        public static string GetConnectionString(string server, int port, string db, string user, string pass)
            => $"Database={db};Data Source={server};Port={port};User Id={user};Password={pass}";

        public string GetConnectionString() => connection.ConnectionString;

        /// <summary>Устанавливает соединение с сервером</summary>
        /// <param name="connectionString">Строка с данными для подключения к базе данных</param>
        /// <returns>Если во время выполнения возникает ошибка, возвращает её текст.</returns>
        public void SetConnection(string connectionString)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
            connection = new MySqlConnection(connectionString);
            command.Connection = connection;
        }

        /// <summary>Устанавливает соединение с сервером</summary>
        /// <param name="connection">Переменная MySqlConnection (клонируется)</param>
        public void SetConnection(MySqlConnection connection)
        {
            if (connection != null && connection.State == ConnectionState.Open)
            {
                connection.Close();
            }
            this.connection = (MySqlConnection)connection.Clone();
            command.Connection = connection;
        }

        private MySqlDataVoid OpenConnection()
        {
            MySqlDataVoid mr = new MySqlDataVoid();
            try
            {
                connection.Open();
                mr.HasError = false;
            }
            catch (MySqlException ex)
            {
                CatchError(mr, ex);
            }
            return mr;
        }

        private void CatchError(MySqlDataVoid mr, MySqlException ex)
        {
            mr.HasError = true;
            mr.Error = MySqlDataError.ParceError(ex, ErrorPattern);
        }

        private void CatchError(MySqlDataVoid mr, Exception ex)
        {
            mr.HasError = true;
            mr.Error = new MySqlDataError(ex);
        }

        #region ReturnOneValue
        /// <summary>Для выполнения запросов с возвращением отдельного значения.</summary>
        /// <param name="query">Текст запроса к базе данных</param>
        /// <returns>Возвращает значение при успешном выполнении запроса или текст ошибки</returns>
        public MySqlDataResult<string> ExecuteScalar(string query) => ExecuteScalar<string>(query);
        /// <summary>Для выполнения запросов с возвращением отдельного значения.</summary>
        /// <param name="query">Текст запроса к базе данных</param>
        /// <returns>Возвращает значение при успешном выполнении запроса или текст ошибки</returns>
        public MySqlDataResult<T> ExecuteScalar<T>(string query)
        {
            MySqlDataVoid result1 = OpenConnection();
            MySqlDataResult<T> result = new MySqlDataResult<T>(default(T), result1.Error, result1.HasError);
            if (result.HasError)
            {
                return result;
            }
            command.CommandText = query;
            try
            {
                object r = command.ExecuteScalar();
                if (r == null)
                {
                    if (default(T) != null) //Nullable.GetUnderlyingType(typeof(T)) == null && typeof(T).IsValueType
                    {
                        throw new NullReferenceException($"Query returned null. Cannot convert null to {typeof(T).Name}");
                    }
                }
                else
                {
                    result.Result = DataRowEx.TypeConverter.Get<T>(r);
                }
                result.HasError = false;
            }
            catch (MySqlException ex)
            {
                CatchError(result, ex);
            }
            catch (Exception ex) when (!ex.Message.StartsWith("Query returned null"))
            {
                CatchError(result, ex);
            }
            finally
            {
                connection.Close();
            }
            if (ClearParametersAfterQuery)
            {
                Parameters.Clear();
            }
            return result;
        }

        /// <summary>
        /// Для выполнения запросов к MySQL без возвращения параметров.
        /// </summary>
        /// <param name="query">Текст запроса к базе данных</param>
        /// <returns>Возвращает количество затронутых строк при успешном выполнении запроса или текст ошибки</returns>
        public MySqlDataResult<int> ExecuteNonQuery(string query)
        {
            MySqlDataVoid result1 = OpenConnection();
            MySqlDataResult<int> result = new MySqlDataResult<int>(0, result1.Error, result1.HasError);
            if (result.HasError)
            {
                return result;
            }
            command.CommandText = query;
            try
            {
                result.Result = command.ExecuteNonQuery();
                result.HasError = false;
            }
            catch (MySqlException ex)
            {
                CatchError(result, ex);
            }
            catch (Exception ex)
            {
                CatchError(result, ex);
            }
            connection.Close();
            if (ClearParametersAfterQuery) Parameters.Clear();
            return result;
        }
        #endregion

        #region ReturnManyValues
        /// <summary>Выполняет запрос выборки набора строк.</summary>
        /// <param name="query">Текст запроса к базе данных</param>
        /// <returns>Возвращает набор строк в DataSet при успешном выполнении запроса или текст ошибки</returns>
        public MySqlDataResult<DataTable> ExecuteReader(string query, bool useEncodingSafeColumnNames = false)
        {
            List<string[]> colAliases = new List<string[]>();
            if (useEncodingSafeColumnNames)
            {
                colAliases = SetEncodingSafeColumnNames(ref query);
            }

            MySqlDataVoid result1 = OpenConnection();
            MySqlDataResult<DataTable> result = new MySqlDataResult<DataTable>(null, result1.Error, result1.HasError);
            if (result.HasError)
            {
                return result;
            }
            command.CommandText = query;
            try
            {
                MySqlDataAdapter AdapterP = new MySqlDataAdapter()
                {
                    SelectCommand = command
                };
                DataSet ds1 = new DataSet();
                AdapterP.Fill(ds1);
                result.Result = ds1.Tables[0];
                result.HasError = false;
            }
            catch (MySqlException ex)
            {
                CatchError(result, ex);
                result.HasError = true;
            }
            catch (Exception ex)
            {
                CatchError(result, ex);
            }
            connection.Close();
            if (ClearParametersAfterQuery) Parameters.Clear();

            foreach (string[] col in colAliases)
            {
                result.Result.Columns[col[0]].ColumnName = col[1];
            }
            return result;
        }

        private List<string[]> SetEncodingSafeColumnNames(ref string query)
        {
            List<string[]> res = new List<string[]>();
            string q = query.Trim().ToLower();
            if (!q.StartsWith("select")) return res;

            // 0 - ?
            // 1 - string
            // 2 - ()
            // 3 - space
            // 4 - row separator
            // 5 - select
            // 6 - from
            int[] map = new int[q.Length];
            map["select".Length - 1] = 5;

            char[] str = { '\'', '"', '`' };
            bool[] strIn = new bool[str.Length];
            int parenthesesDepth = 0;

            Point lastStr = new Point(-1, -1);
            Point lastSpace = new Point(-1, -1);
            Point lastPar = new Point(-1, -1);
            for (int cursor = 6; cursor < q.Length && map[cursor - 1] != 6; cursor++)
            {
                char c = q[cursor];

                // strings
                if (strIn.Count(x => x == true) > 0)
                {
                    map[cursor] = parenthesesDepth == 0 ? 1 : 2;

                    int sInd = -1;
                    for (int i = 0; i < str.Length; i++)
                    {
                        if (strIn[i])
                        {
                            sInd = i;
                            break;
                        }
                    }
                    if (c == str[sInd])
                    {
                        strIn[sInd] = false;
                    }
                    continue;
                }
                else if (str.Contains(c))
                {
                    map[cursor] = parenthesesDepth == 0 ? 1 : 2;

                    for (int i = 0; i < str.Length; i++)
                    {
                        if (str[i] == c)
                        {
                            strIn[i] = true;
                            break;
                        }
                    }
                    continue;
                }

                // Parentheses
                if (c == '(')
                {
                    map[cursor] = 2;

                    parenthesesDepth++;
                    continue;
                }
                else if (c == ')')
                {
                    map[cursor] = 2;

                    parenthesesDepth--;
                    continue;
                }

                if (parenthesesDepth > 0)
                {
                    map[cursor] = 2;
                    continue;
                }

                // Spaces
                if (c == ' ')
                {
                    map[cursor] = 3;
                    continue;
                }
                
                // Column separators or SELECT area ended
                if (c == ',' || q.Substring(cursor, "from".Length) == "from")
                {
                    bool lastCol = false;
                    if (c == ',')
                    {
                        map[cursor] = 4;
                    }
                    else
                    {
                        map[cursor] = 6;
                        lastCol = true;
                    }

                    int from, to;
                    from = cursor - 1;

                    while (map[from] == 3) from--;
                    to = from;

                    if (map[to] == 1 || map[to] == 0)
                    {
                        // alias inside string (1) or plain text (0)
                        while (map[from] == map[to]) from--;
                        from++;
                    }
                    else
                    {
                        continue;
                    }

                    // checking is there is alias at all
                    int before = from - 1;
                    while (map[before] == 3) before--;
                    if (map[before] == 4 || map[before] == 5)
                    {
                        // there is no alias
                        continue;
                    }

                    // changing alias
                    string colOldAlias = query.Substring(from, to - from).Trim(str);
                    string colNewAlias = $"'col{res.Count}'";
                    res.Add(new string[] { colNewAlias.Trim('\''), colOldAlias });
                    q = q.Remove(from, to - from + 1).Insert(from, colNewAlias);
                    query = query.Remove(from, to - from + 1).Insert(from, colNewAlias);

                    if (!lastCol)
                    {
                        cursor = from + colNewAlias.Length;
                    }

                    continue;
                }
            }
            
            return res;
        }
        #endregion

        /*#region Backup&Restore
        /// <summary>Создаёт резервную копию базы данных</summary>
        /// <param name="pathToMysqldump">Путь к mysqldump.exe</param>
        /// <param name="path">Путь, по которому она будет сохранена</param>
        /// <param name="password">Пароль пользователя MySql сервера. null - если вы хотите ввести пароль напрямую в консоли</param>
        /// <param name="name">Имя файла с резервной копией. null - Установить значение по умолчанию</param>
        /// <returns>Если во время выполнения возникает ошибка, возвращает её текст.</returns>
        public MyResult<bool> Backup(string pathToMysqldump, string path, string password = null, string name = null)
        {
            MyResult<bool> mr = new MyResult<bool>();
            if (!path.EndsWith("\\"))
            {
                path += "\\";
            }
            if (!Directory.Exists(path))
            {
                mr.HasError = true;
                mr.Error = "Directory doesn't exists";
                return mr;
            }
            try
            {
                MySqlConnectionStringBuilder sb = new MySqlConnectionStringBuilder(connection.ConnectionString);
                string uid = sb.UserID,
                    server = sb.Server,
                    database = sb.Database;

                if (name == null)
                {
                    path += DateTime.Now.ToString("yyyy.MM.dd HH:mm:ss") + ".sql";
                }
                else
                {
                    path += name;
                }

                ProcessStartInfo psi = new ProcessStartInfo()
                {
                    FileName = pathToMysqldump,
                    RedirectStandardInput = false,
                    RedirectStandardOutput = true,
                    Arguments = string.Format(@"-u{0} -p{1} -h{2} {3}", uid, password, server, database),
                    UseShellExecute = false
                };
                Process process = Process.Start(psi);

                string output;
                output = process.StandardOutput.ReadToEnd();
                process.WaitForExit();
                if (process.ExitCode != 0)
                {
                    process.Close();
                    throw new ArgumentException();
                }
                StreamWriter file = new StreamWriter(path);
                file.WriteLine(output);
                file.Close();
                process.Close();
                mr.HasError = false;
            }
            catch (IOException ex)
            {
                mr.Error = ex.Message;
                mr.HasError = true;
            }
            catch (Exception ex)
            {
                mr.HasError = true;
                mr.Error = ex.Message;
            }
            return mr;
        }

        /// <summary>Восстанавливает резервную копию базы данных из файла</summary>
        /// <param name="pathToMysql">Путь к mysql.exe</param>
        /// <param name="path">Путь, по которому расположен файл</param>
        /// <param name="password">Пароль пользователя MySql сервера. null - если вы хотите ввести пароль напрямую в консоли</param>
        /// <returns>Если во время выполнения возникает ошибка, возвращает её текст</returns>
        public MyResult<bool> Restore(string pathToMysql, string path, string password = null)
        {
            MyResult<bool> mr = new MyResult<bool>();
            if (!File.Exists(path))
            {
                mr.HasError = true;
                mr.Error = "File doesn't exists";
                return mr;
            }
            try
            {
                MySqlConnectionStringBuilder sb = new MySqlConnectionStringBuilder(connection.ConnectionString);
                string uid = sb.UserID,
                    server = sb.Server,
                    database = sb.Database;

                StreamReader file = new StreamReader(path);
                string input = file.ReadToEnd();
                file.Close();

                ProcessStartInfo psi = new ProcessStartInfo()
                {
                    FileName = pathToMysql,
                    RedirectStandardInput = true,
                    RedirectStandardOutput = false,
                    Arguments = string.Format(@"-u{0} -p{1} -h{2} {3}", uid, password, server, database),
                    UseShellExecute = false
                };
                Process process = Process.Start(psi);
                process.StandardInput.WriteLine(input);
                process.StandardInput.Close();
                process.WaitForExit();
                if (process.ExitCode != 0)
                {
                    process.Close();
                    throw new ArgumentException();
                }
                process.Close();
                mr.HasError = false;
            }
            catch (IOException ex)
            {
                mr.HasError = true;
                mr.Error = ex.Message;
            }
            catch (Exception ex)
            {
                mr.HasError = true;
                mr.Error = ex.Message;
            }
            return mr;
        }
        #endregion*/
    }

    public static class DataRowEx
    {
        public static T Get<T>(this DataRow row, int columnIndex, bool DefaultIfNull = false)
        {
            return TypeConverter.Get<T>(row[columnIndex], DefaultIfNull);
        }

        public static T Get<T>(this DataRow row, string columnName, bool DefaultIfNull = false)
        {
            return TypeConverter.Get<T>(row[columnName], DefaultIfNull);
        }

        public static bool IsNull(this DataRow row, int index)
        {
            return TypeConverter.IsNull(row[index]);
        }

        public static class TypeConverter
        {
            public static T Get<T>(object value, bool DefaultIfNull = false)
            {
                Type type = typeof(T);
                Type underlyingType = Nullable.GetUnderlyingType(type);
                if ((DefaultIfNull || underlyingType != null) && IsNull(value))
                {
                    return default(T);
                }
                if (underlyingType != null)
                {
                    type = underlyingType;
                }
                return (T)Convert.ChangeType(value, type);
            }

            public static bool IsNull(object value)
            {
                return value == null || value == DBNull.Value;
            }
        }
    }
}