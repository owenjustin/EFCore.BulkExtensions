using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Metadata.Internal;

namespace EFCore.BulkExtensions
{
    public static class OwnedTypeUtil
    {
        public static bool IsOwnedInSameTableAsOwner(IEntityType owned)
        {
            var ownership = owned.FindOwnership();

            if (ownership is null)
                return false;

            var owner = ownership.PrincipalEntityType;
            var ownedTables = owned.GetTableMappings();

            foreach (var ot in ownedTables)
            {
                var isSharingTable = ot.Table.EntityTypeMappings.Any(y => y.EntityType == owner);

                if (isSharingTable == false)
                    return false;
            }

            return true;
        }

        public static bool IsOwnedInSameTableAsOwner(INavigation navigation)
        {
            return IsOwnedInSameTableAsOwner(navigation.DeclaringEntityType);
        }
    }
}
