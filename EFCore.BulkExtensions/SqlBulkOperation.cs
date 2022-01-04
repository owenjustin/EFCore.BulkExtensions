using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using EFCore.BulkExtensions.SQLAdapters;
using FastMember;
using Microsoft.Data.Sqlite;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace EFCore.BulkExtensions
{
    public enum DbServer
    {
        SqlServer,
        MySql,
        PostrgeSql,
        Sqlite,
    }

    public enum OperationType
    {
        Insert,
        InsertOrUpdate,
        InsertOrUpdateOrDelete,
        Update,
        Delete,
        Read,
        Truncate,
        SaveChanges,
    }

    public class SqlProviderNotSupportedException : NotSupportedException
    {
        public SqlProviderNotSupportedException(string providerName, string message = null) : base($"Provider {providerName} not supported. Only SQL Server and SQLite are Currently supported. {message}") { }
    }

    internal static class SqlBulkOperation
    {
        internal static string ColumnMappingExceptionMessage => "The given ColumnMapping does not match up with any column in the source or destination";

        public static void Insert<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Insert(context, type, entities, tableInfo, progress);
        }

        public static async Task InsertAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.InsertAsync(context, type, entities, tableInfo, progress, cancellationToken);
        }

        public static void Merge<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Merge(context, type, entities, tableInfo, operationType, progress);
        }

        public static async Task MergeAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, OperationType operationType, Action<decimal> progress, CancellationToken cancellationToken = default) where T : class
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.MergeAsync(context, type, entities, tableInfo, operationType, progress, cancellationToken);
        }

        public static void Read<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress) where T : class
        {
            if (tableInfo.BulkConfig.UseTempDB) // dropTempTableIfExists
            {
                context.Database.ExecuteSqlCommand(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB));
            }
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Read(context, type, entities, tableInfo, progress);
        }

        public static async Task ReadAsync<T>(DbContext context, Type type, IList<T> entities, TableInfo tableInfo, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
            if (tableInfo.BulkConfig.UseTempDB) // dropTempTableIfExists
            {
                await context.Database.ExecuteSqlCommandAsync(SqlQueryBuilder.DropTable(tableInfo.FullTempTableName, tableInfo.BulkConfig.UseTempDB), cancellationToken).ConfigureAwait(false);
            }
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.ReadAsync(context, type, entities, tableInfo, progress, cancellationToken);
        }

        public static void Truncate(DbContext context, TableInfo tableInfo)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            adapter.Truncate(context, tableInfo);
        }

        public static async Task TruncateAsync(DbContext context, TableInfo tableInfo, CancellationToken cancellationToken)
        {
            var adapter = SqlAdaptersMapping.CreateBulkOperationsAdapter(context);
            await adapter.TruncateAsync(context, tableInfo, cancellationToken).ConfigureAwait(false);
        }
    }

}
