using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.EntityFrameworkCore;

namespace EFCore.BulkExtensions
{
    internal static class DbContextBulkTransaction
    {
        public static void Execute<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress) where T : class
        {
            
                if (entities.Count == 0 && operationType != OperationType.InsertOrUpdateOrDelete && operationType != OperationType.Truncate && operationType != OperationType.SaveChanges)
                {
                    return;
                }

                if (operationType == OperationType.SaveChanges)
                {
                    DbContextBulkTransactionSaveChanges.SaveChanges(context, bulkConfig, progress);
                    return;
                }
                else if (bulkConfig?.IncludeGraph == true)
                {
                    //DbContextBulkTransactionGraphUtil.ExecuteWithGraph(context, entities, operationType, bulkConfig, progress);
                }
                else
                {
                    TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

                    if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                    {
                        SqlBulkOperation.Insert(context, entities, tableInfo, progress);
                    }
                    else if (operationType == OperationType.Read)
                    {
                        SqlBulkOperation.Read(context, entities, tableInfo, progress);
                    }
                    //else if (operationType == OperationType.Truncate)
                    //{
                    //    SqlBulkOperation.Truncate(context, tableInfo);
                    //}
                    else
                    {
                        SqlBulkOperation.Merge(context, entities, tableInfo, operationType, progress);
                    }
                }
            
            //if (entities.Count == 0)
            //{
            //    return;
            //}
            //TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            //if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            //{
            //    SqlBulkOperation.Insert(context, entities, tableInfo, progress);
            //}
            //else if (operationType == OperationType.Read)
            //{
            //    SqlBulkOperation.Read(context, entities, tableInfo, progress);
            //}
            //else
            //{
            //    SqlBulkOperation.Merge(context, entities, tableInfo, operationType, progress);
            //}
        }

        public static async Task ExecuteAsync<T>(DbContext context, IList<T> entities, OperationType operationType, BulkConfig bulkConfig, Action<decimal> progress, CancellationToken cancellationToken) where T : class
        {
                if (entities.Count == 0 && operationType != OperationType.InsertOrUpdateOrDelete && operationType != OperationType.Truncate && operationType != OperationType.SaveChanges)
                {
                    return;
                }

                if (operationType == OperationType.SaveChanges)
                {
                    await DbContextBulkTransactionSaveChanges.SaveChangesAsync(context, bulkConfig, progress, cancellationToken);
                }
                else if (bulkConfig?.IncludeGraph == true)
                {
                    //await DbContextBulkTransactionGraphUtil.ExecuteWithGraphAsync(context, entities, operationType, bulkConfig, progress, cancellationToken);
                }
                else
                {
                    TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

                    if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
                    {
                        await SqlBulkOperation.InsertAsync(context, entities, tableInfo, progress, cancellationToken);
                    }
                    else if (operationType == OperationType.Read)
                    {
                        await SqlBulkOperation.ReadAsync(context, entities, tableInfo, progress, cancellationToken);
                    }
                    //else if (operationType == OperationType.Truncate)
                    //{
                    //    await SqlBulkOperation.TruncateAsync(context, tableInfo, cancellationToken);
                    //}
                    else
                    {
                        await SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress, cancellationToken);
                    }
                }
            
            //if (entities.Count == 0)
            //{
            //    return Task.CompletedTask;
            //}
            //TableInfo tableInfo = TableInfo.CreateInstance(context, entities, operationType, bulkConfig);

            //if (operationType == OperationType.Insert && !tableInfo.BulkConfig.SetOutputIdentity)
            //{
            //    return SqlBulkOperation.InsertAsync(context, entities, tableInfo, progress, cancellationToken);
            //}
            //else if (operationType == OperationType.Read)
            //{
            //    return SqlBulkOperation.ReadAsync(context, entities, tableInfo, progress, cancellationToken);
            //}
            //else
            //{
            //    return SqlBulkOperation.MergeAsync(context, entities, tableInfo, operationType, progress, cancellationToken);
            //}
        }
    }
}
