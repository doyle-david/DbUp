using System;
using System.Collections.Generic;
using System.IO;
using System.Text;

namespace DbUp.Engine
{
    /// <summary>
    /// Represents a SQL Server script that is fetched in chunks at execution time, rather than discovery time
    /// </summary>
    public class LargeSqlScript : SqlScript, IDisposable
    {
        private readonly FileStream fileStream;
        private readonly StreamReader resourceStreamReader;
        private string nextScriptStart;

        /// <summary>
        /// Initializes a new instance of the <see cref="LargeSqlScript"/> class.
        /// </summary>
        /// <param name="name">The name.</param>
        public LargeSqlScript(string name)
            : base(name, null)
        {
            fileStream = new FileStream(Name, FileMode.Open, FileAccess.Read);
            resourceStreamReader = new StreamReader(fileStream, Encoding.Default, true);
        }

        /// <summary>
        /// Gets the contents of the script.
        /// </summary>
        public override string Contents
        {
            get
            {
                var lines = new List<string>();

                var newLine = GetFirstLine();

                LoadLinesUntilFirstInsert(newLine, lines);

                LoadInsertsForCurrentTable(newLine, lines);

                AddCommitAndGo(lines);

                string contents;

                try
                {
                    System.Diagnostics.Debug.WriteLine(string.Format("Lines Count: {0}", lines.Count));

                    contents = string.Join("\r\n", lines.ToArray());
                    contents = AddBeginTran(contents);
                }
                catch (Exception ex)
                {
                    System.Diagnostics.Debug.WriteLine(ex.ToString());
                    throw;
                }

                // return the next contents
                return contents;
            }
        }

        private string GetFirstLine()
        {
            var newLine = !string.IsNullOrEmpty(nextScriptStart) ? nextScriptStart : resourceStreamReader.ReadLine();
            nextScriptStart = null;
            return newLine;
        }

        private void LoadLinesUntilFirstInsert(string newLine, List<string> lines)
        {
            while (!string.IsNullOrEmpty(newLine) && !newLine.StartsWith("INSERT INTO"))
            {
                lines.Add(newLine);
                newLine = resourceStreamReader.ReadLine();
            }
        }

        private void LoadInsertsForCurrentTable(string newLine, List<string> lines)
        {
            if (!string.IsNullOrEmpty(newLine))
            {
                var tableName = GetTableName(newLine);

                // Get the rest of the inserts for that table
                while (!string.IsNullOrEmpty(newLine) && newLine.Contains(tableName) && lines.Count < 131072)
                {
                    // If we don't have the full insert statement then get it from the next line
                    if (newLine.StartsWith("INSERT INTO") && !newLine.EndsWith(")"))
                    {
                        newLine = newLine + resourceStreamReader.ReadLine();
                    }

                    lines.Add(newLine);
                    newLine = resourceStreamReader.ReadLine();
                }

                // Save the line that does not match for the next script contents
                nextScriptStart = newLine;
            }
        }

        private static string GetTableName(string newLine)
        {
            // Get start of table name where table name like [domain].[TableName]
            int tableNameStart = newLine.IndexOf(".[", StringComparison.CurrentCultureIgnoreCase) + 2;

            // Get table name length where insert statement like INSERT INTO [domain].[TableName] (ColumnName...
            int tableNameLength = newLine.IndexOf("(", StringComparison.CurrentCultureIgnoreCase) - tableNameStart - 2;
            return newLine.Substring(tableNameStart, tableNameLength);
        }

        private static void AddCommitAndGo(List<string> lines)
        {
            if (lines.Count == 0)
            {
                return;
            }

            const string commitTransaction = "COMMIT TRANSACTION";
            if (!lines[lines.Count - 1].Equals(commitTransaction, StringComparison.CurrentCultureIgnoreCase))
            {
                lines.Add(commitTransaction);
            }

            const string sqlGoStatement = "GO";
            lines.Add(sqlGoStatement);
        }

        private static string AddBeginTran(string contents)
        {
            const string beginTransaction = "BEGIN TRANSACTION";
            if (!string.IsNullOrEmpty(contents) && contents.IndexOf(beginTransaction, StringComparison.CurrentCultureIgnoreCase) < 0)
            {
                contents = contents.Insert(0, string.Format("{0}\r\n", beginTransaction));
            }

            return contents;
        }

        public void Dispose()
        {
            fileStream.Dispose();
            resourceStreamReader.Dispose();
        }
    }
}
